# -*- coding: utf-8 -*-
import os, sys, csv, json, requests, re

WIX_API = "https://www.wixapis.com/stores/v1"
API_KEY = os.environ.get("WIX_API_KEY", "").strip()
SITE_ID = os.environ.get("WIX_SITE_ID", "").strip()

HEADERS = {
    "Authorization": API_KEY,
    "wix-site-id": SITE_ID,
    "Content-Type": "application/json"
}

def log(*a): 
    print(*a, flush=True)

def round_price(x): 
    return float(f"{x:.2f}")

OPTION_NAME = "PREORDER PAYMENTS OPTIONS*"
CHOICE_DEPOSIT = "ANTICIPO/SALDO"
CHOICE_PREPAY  = "PAGAMENTO ANTICIPATO"

# ---------------- CSV PATH ----------------
def resolve_csv_path():
    cand = []
    if len(sys.argv) > 1 and sys.argv[1].strip():
        cand.append(sys.argv[1].strip())
    if os.environ.get("CSV_PATH"):
        cand.append(os.environ["CSV_PATH"].strip())
    cand += [
        "input/template_preordini_v7.csv",
        "template_preordini_v7.csv",
        "data/template_preordini_v7.csv",
        "csv/template_preordini_v7.csv"
    ]
    for p in cand:
        if p and os.path.exists(p):
            return p
    raise FileNotFoundError(f"CSV non trovato. Provati: {cand}")

# ---------------- NORMALIZZAZIONE CATEGORIE ----------------
def norm(s: str) -> str:
    s = (s or "").strip().casefold()
    # rimuovo tutto tranne alnum/spazi
    s = "".join(ch for ch in s if ch.isalnum() or ch.isspace())
    s = re.sub(r"\s+", " ", s).strip()
    return s

# ---------------- COLLECTIONS ----------------
def get_collections_map():
    url = f"{WIX_API}/collections/query"
    out, cursor = {}, None
    while True:
        payload = {"query": {"paging": {"limit": 100}}}
        if cursor:
            payload["query"]["paging"]["cursor"] = cursor
        r = requests.post(url, headers=HEADERS, data=json.dumps(payload), timeout=30)
        r.raise_for_status()
        data = r.json()
        for c in data.get("collections", []):
            name = (c.get("name") or "").strip()
            cid = c.get("id")
            if name and cid:
                out[norm(name)] = cid
        cursor = data.get("paging", {}).get("nextCursor")
        if not cursor:
            break
    return out

# ---------------- QUERY PRODOTTO ----------------
def query_product_by_sku(sku):
    url = f"{WIX_API}/products/query"
    for payload in (
        {"query": {"filter": {"sku": {"$eq": sku}}, "paging": {"limit": 1}}},
        {"query": {"filter": {"sku": sku}, "paging": {"limit": 1}}},
    ):
        try:
            r = requests.post(url, headers=HEADERS, data=json.dumps(payload), timeout=20)
            if r.ok:
                items = r.json().get("products", [])
                if items: 
                    return items[0]
        except:
            pass
    # fallback: scansione paginata
    cursor = None
    while True:
        payload = {"query": {"paging": {"limit": 100}}}
        if cursor:
            payload["query"]["paging"]["cursor"] = cursor
        r = requests.post(url, headers=HEADERS, data=json.dumps(payload), timeout=30)
        if not r.ok: break
        data = r.json()
        for p in data.get("products", []):
            if (p.get("sku") or "").strip().lower() == sku.strip().lower():
                return p
        cursor = data.get("paging", {}).get("nextCursor")
        if not cursor: break
    return None

# ---------------- OPZIONI ----------------
def options_payload():
    return [{
        "name": OPTION_NAME,
        "choices": [
            {"value": CHOICE_DEPOSIT, "description": CHOICE_DEPOSIT},
            {"value": CHOICE_PREPAY,  "description": CHOICE_PREPAY}
        ]
    }]

# ---------------- DESCRIZIONE ----------------
def build_description(row):
    descr_raw = (row.get("descrizione") or "").strip()
    # accetto entrambe le intestazioni possibili per sicurezza
    dl  = (row.get("preorder_deadline") or row.get("deadline") or "").strip()
    eta = (row.get("eta") or row.get("uscita_prevista") or "").strip()

    head = []
    if dl:  head.append(f"<strong>PREORDER DEADLINE:</strong> {dl}")
    if eta: head.append(f"<strong>ETA:</strong> {eta}")

    head_html = "<br>".join(head) if head else ""
    body_html = (descr_raw.replace("\n", "<br>") if descr_raw else "").strip()

    if head_html and body_html:
        # riga vuota tra testata e corpo
        return f"<p>{head_html}</p><p><br></p><p>{body_html}</p>"
    elif head_html:
        return f"<p>{head_html}</p>"
    elif body_html:
        return f"<p>{body_html}</p>"
    return ""

# ---------------- CREAZIONE / PATCH PRODOTTO ----------------
def create_product(row, collection_id=None):
    name = (row.get("nome_articolo") or "").strip()
    if len(name) > 80: 
        log("[WARN] Nome > 80 caratteri, troncato.")
        name = name[:80]
    sku   = (row.get("sku") or "").strip()
    price = float(str(row.get("prezzo_eur") or "0").replace(",", "."))
    brand = (row.get("brand") or "").strip() or None

    product = {
        "name": name,
        "sku": sku,
        "productType": "physical",
        "priceData": {"price": round_price(price)},
        "description": build_description(row),
        "brand": brand,
        "ribbon": "PREORDER",
        "tags": ["PREORDER"],
        "productOptions": options_payload(),
        "manageVariants": True
    }
    if collection_id:
        product["collectionIds"] = [collection_id]

    r = requests.post(f"{WIX_API}/products", headers=HEADERS,
                      data=json.dumps({"product": product}), timeout=60)
    if not r.ok:
        raise RuntimeError(f"POST /products failed {r.status_code}: {r.text}")
    return r.json().get("product", {}).get("id")

def patch_product_main(pid, row, price, collection_id):
    name = (row.get("nome_articolo") or "").strip()
    if len(name) > 80: 
        name = name[:80]
    brand = (row.get("brand") or "").strip() or None

    patch = {
        "product": {
            "id": pid,
            "name": name,
            "description": build_description(row),
            "brand": brand,
            "priceData": {"price": round_price(price)},
            "ribbon": "PREORDER",
            "tags": ["PREORDER"]
        }
    }
    if collection_id:
        patch["product"]["collectionIds"] = [collection_id]

    r = requests.patch(f"{WIX_API}/products/{pid}", headers=HEADERS,
                       data=json.dumps(patch), timeout=60)
    if not r.ok:
        raise RuntimeError(f"PATCH /products/{pid} failed {r.status_code}: {r.text}")

def ensure_manage_variants(product_id):
    patch = {"product": {"id": product_id, "manageVariants": True}}
    r = requests.patch(f"{WIX_API}/products/{product_id}", headers=HEADERS,
                       data=json.dumps(patch), timeout=30)
    if not r.ok:
        raise RuntimeError(f"PATCH manageVariants failed {r.status_code}: {r.text}")

def force_options(product_id):
    patch = {"product": {"id": product_id, "productOptions": options_payload()}}
    r = requests.patch(f"{WIX_API}/products/{product_id}", headers=HEADERS,
                       data=json.dumps(patch), timeout=30)
    if not r.ok:
        raise RuntimeError(f"PATCH options failed {r.status_code}: {r.text}")

# ---------------- VARIANTS ----------------
def get_variants(product_id):
    # 1) tentativo endpoint variants dedicato
    url = f"{WIX_API}/products/{product_id}/variants"
    r = requests.get(url, headers=HEADERS, timeout=30)
    if r.ok:
        return r.json().get("variants", [])
    # 2) fallback: leggo il prodotto intero e prendo variants se presenti
    url = f"{WIX_API}/products/{product_id}"
    r = requests.get(url, headers=HEADERS, timeout=30)
    if r.ok:
        return r.json().get("product", {}).get("variants", [])
    return []

def patch_variant_prices_by_id(product_id, base_price, base_sku):
    deposit = round_price(base_price * 0.30)
    prepay  = round_price(base_price * 0.95)

    variants = get_variants(product_id)
    if not variants:
        raise RuntimeError("Nessuna variant generata: gestione varianti non attiva oppure opzioni mancanti.")

    id_dep = id_pp = None
    for v in variants:
        # choices come dict: { OPTION_NAME: VALUE }
        ch = v.get("choices") or v.get("options") or {}
        val = ch.get(OPTION_NAME)
        if val == CHOICE_DEPOSIT:
            id_dep = v.get("id")
        elif val == CHOICE_PREPAY:
            id_pp = v.get("id")

    payload_variants = []
    if id_dep:
        payload_variants.append({
            "id": id_dep,
            "priceData": {"price": deposit},
            "sku": f"{base_sku}-DEP"
        })
    if id_pp:
        payload_variants.append({
            "id": id_pp,
            "priceData": {"price": prepay, "compareAtPrice": round_price(base_price)},
            "sku": f"{base_sku}-PREPAY"
        })

    if not payload_variants:
        # fallback: patch via choices (se ID non sono stati trovati)
        payload_variants = [
            {
                "choices": { OPTION_NAME: CHOICE_DEPOSIT },
                "priceData": {"price": deposit},
                "sku": f"{base_sku}-DEP"
            },
            {
                "choices": { OPTION_NAME: CHOICE_PREPAY },
                "priceData": {"price": prepay, "compareAtPrice": round_price(base_price)},
                "sku": f"{base_sku}-PREPAY"
            }
        ]

    r = requests.patch(f"{WIX_API}/products/{product_id}/variants",
                       headers=HEADERS, data=json.dumps({"variants": payload_variants}), timeout=60)
    if not r.ok:
        raise RuntimeError(f"PATCH /products/{product_id}/variants failed {r.status_code}: {r.text}")

def try_enable_preorder(product_id):
    url = f"{WIX_API}/products/{product_id}"
    for p in [
        {"product": {"isPreOrder": True}},
        {"product": {"preorderInfo": {"isPreOrder": True}}},
        {"product": {"preOrder": True}},
    ]:
        r = requests.patch(url, headers=HEADERS, data=json.dumps(p), timeout=15)
        if r.ok: 
            return True
    return False

def add_to_collections(product_id, collection_id):
    if not collection_id: 
        return
    payload = {"productIds": [product_id]}
    r = requests.post(f"{WIX_API}/collections/{collection_id}/products/add",
                      headers=HEADERS, data=json.dumps(payload), timeout=20)
    # anche se non 200, non blocchiamo: abbiamo già messo collectionIds nel prodotto

# ---------------- MAIN ----------------
def main():
    if not API_KEY or not SITE_ID:
        print("[FATAL] WIX_API_KEY / WIX_SITE_ID mancanti"); sys.exit(2)

    log("[PRECHECK] API ok. Inizio…")
    # categorie
    try:
        col_map = get_collections_map()
        log("[INFO] Categorie caricate:", ", ".join(list(col_map.keys())[:20]))
    except Exception as e:
        log(f"[WARN] Lettura categorie fallita: {e}")
        col_map = {}

    csv_path = resolve_csv_path()
    log(f"[INFO] CSV: {csv_path}")

    created = updated = 0
    with open(csv_path, "r", encoding="utf-8-sig", newline="") as fh:
        rd = csv.DictReader(fh, delimiter=";")
        required = {"nome_articolo", "prezzo_eur", "sku"}
        headers_norm = { (h or "").strip(): h for h in (rd.fieldnames or []) }
        if not rd.fieldnames or not required.issubset(set(headers_norm.keys())):
            miss = sorted(list(required - set(headers_norm.keys())))
            print(f"[FATAL] CSV mancano colonne: {miss}"); sys.exit(2)

        for i, row in enumerate(rd, start=2):
            name = (row.get("nome_articolo") or "").strip()
            sku  = (row.get("sku") or "").strip()
            price_raw = row.get("prezzo_eur")
            if not name or not sku or not price_raw:
                log(f"[SKIP] Riga {i}: dati incompleti."); 
                continue
            try:
                price = float(str(price_raw).replace(",", "."))
            except:
                log(f"[SKIP] Riga {i}: prezzo non valido."); 
                continue

            # categoria
            cat_id = None
            raw_cat = (row.get("categoria") or "").strip()
            if raw_cat and col_map:
                cat_id = col_map.get(norm(raw_cat))
                if not cat_id:
                    log(f"[WARN] Categoria '{raw_cat}' non trovata.")

            existing = query_product_by_sku(sku)

            try:
                if existing:
                    pid = existing["id"]
                    # patch base
                    patch_product_main(pid, row, price, cat_id)
                    # opzioni + varianti abilitate
                    force_options(pid)
                    ensure_manage_variants(pid)
                    # prezzi varianti per ID
                    patch_variant_prices_by_id(pid, price, sku)
                    try_enable_preorder(pid)
                    add_to_collections(pid, cat_id)
                    updated += 1
                    log(f"[OK] Aggiornato: {name}")
                else:
                    pid = create_product(row, collection_id=cat_id)
                    force_options(pid)
                    ensure_manage_variants(pid)
                    patch_variant_prices_by_id(pid, price, sku)
                    try_enable_preorder(pid)
                    add_to_collections(pid, cat_id)
                    created += 1
                    log(f"[OK] Creato: {name}")
            except Exception as e:
                log(f"[ERRORE] Riga {i} '{name}': {e}")

    if not created and not updated:
        print("[ERRORE] Nessun prodotto creato/aggiornato."); sys.exit(2)
    print(f"[DONE] Creati: {created}, Aggiornati: {updated}")
    sys.exit(0)

if __name__ == "__main__":
    main()
