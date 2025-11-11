# -*- coding: utf-8 -*-
import os, sys, csv, json, requests, re

WIX_API_BASE = "https://www.wixapis.com/stores/v1"
API_KEY = os.environ.get("WIX_API_KEY", "").strip()
SITE_ID = os.environ.get("WIX_SITE_ID", "").strip()

HEADERS = {
    "Authorization": API_KEY,
    "wix-site-id": SITE_ID,
    "Content-Type": "application/json"
}

def log(*a):
    print(*a, flush=True)

def norm(s: str) -> str:
    s = (s or "").strip().casefold()
    s = "".join(ch for ch in s if ch.isalnum() or ch.isspace())
    s = re.sub(r"\s+", " ", s).strip()
    return s

def round_price(x):
    return float(f"{float(x):.2f}")

OPTION_NAME = "PREORDER PAYMENTS OPTIONS*"
CHOICE_DEPOSIT = "ANTICIPO/SALDO"
CHOICE_PREPAY  = "PAGAMENTO ANTICIPATO"

def resolve_csv_path():
    # ordine: argomento → env → percorsi standard
    candidates = []
    if len(sys.argv) > 1 and sys.argv[1].strip():
        candidates.append(sys.argv[1].strip())
    if os.environ.get("CSV_PATH"):
        candidates.append(os.environ["CSV_PATH"].strip())
    candidates += [
        "input/template_preordini_v7.csv",
        "template_preordini_v7.csv",
        "data/template_preordini_v7.csv",
        "csv/template_preordini_v7.csv"
    ]
    for p in candidates:
        if p and os.path.exists(p):
            return p
    raise FileNotFoundError(f"CSV non trovato. Provati: {candidates}")

def api_get(url, **kw):
    r = requests.get(url, headers=HEADERS, timeout=kw.pop("timeout", 30), **kw)
    return r

def api_post(url, payload, **kw):
    r = requests.post(url, headers=HEADERS, data=json.dumps(payload),
                      timeout=kw.pop("timeout", 60), **kw)
    return r

def api_patch(url, payload, **kw):
    r = requests.patch(url, headers=HEADERS, data=json.dumps(payload),
                       timeout=kw.pop("timeout", 60), **kw)
    return r

# -------- Collections --------
def get_collections_map():
    url = f"{WIX_API_BASE}/collections/query"
    out, cursor = {}, None
    while True:
        payload = {"query": {"paging": {"limit": 100}}}
        if cursor:
            payload["query"]["paging"]["cursor"] = cursor
        r = api_post(url, payload, timeout=30)
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

# -------- Products list (pre-carica TUTTO) --------
def list_all_products_by_sku():
    url = f"{WIX_API_BASE}/products"
    cursor = None
    sku_map = {}
    total = 0
    while True:
        params = {"limit": 100}
        if cursor:
            params["cursor"] = cursor
        r = api_get(url, params=params, timeout=30)
        if not r.ok:
            break
        data = r.json()
        items = data.get("products", [])
        for p in items:
            sku = (p.get("sku") or "").strip().lower()
            if sku:
                sku_map[sku] = p
        total += len(items)
        cursor = data.get("paging", {}).get("nextCursor")
        if not cursor:
            break
    log(f"[PRECHECK] API ok. Prodotti visibili: {len(sku_map)}")
    return sku_map

# -------- Description builder --------
def build_description(row):
    dl  = (row.get("preorder_deadline") or row.get("deadline") or "").strip()
    eta = (row.get("eta") or row.get("uscita_prevista") or "").strip()
    descr_raw = (row.get("descrizione") or "").strip()
    head = []
    if dl:
        head.append(f"<strong>PREORDER DEADLINE:</strong> {dl}")
    if eta:
        head.append(f"<strong>ETA:</strong> {eta}")
    head_html = "<br>".join(head) if head else ""
    body_html = descr_raw.replace("\n", "<br>") if descr_raw else ""

    if head_html and body_html:
        return f"<p>{head_html}</p><p><br></p><p>{body_html}</p>"
    elif head_html:
        return f"<p>{head_html}</p>"
    elif body_html:
        return f"<p>{body_html}</p>"
    return ""

def options_payload():
    return [{
        "name": OPTION_NAME,
        "choices": [
            {"value": CHOICE_DEPOSIT, "description": CHOICE_DEPOSIT},
            {"value": CHOICE_PREPAY,  "description": CHOICE_PREPAY}
        ]
    }]

def ensure_manage_variants(product_id):
    r = api_patch(f"{WIX_API_BASE}/products/{product_id}",
                  {"product": {"id": product_id, "manageVariants": True}})
    if not r.ok:
        raise RuntimeError(f"PATCH manageVariants failed {r.status_code}: {r.text}")

def force_options(product_id):
    r = api_patch(f"{WIX_API_BASE}/products/{product_id}",
                  {"product": {"id": product_id, "productOptions": options_payload()}})
    if not r.ok:
        raise RuntimeError(f"PATCH options failed {r.status_code}: {r.text}")

def get_variants(product_id):
    r = api_get(f"{WIX_API_BASE}/products/{product_id}/variants", timeout=30)
    if r.ok:
        return r.json().get("variants", [])
    r = api_get(f"{WIX_API_BASE}/products/{product_id}", timeout=30)
    if r.ok:
        return r.json().get("product", {}).get("variants", [])
    return []

def patch_variant_prices_by_id(product_id, base_price, base_sku):
    deposit = round_price(base_price * 0.30)
    prepay  = round_price(base_price * 0.95)
    variants = get_variants(product_id)
    if not variants:
        raise RuntimeError("Nessuna variant presente (manageVariants non attivo o opzioni assenti).")

    id_dep = id_pp = None
    for v in variants:
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
        # fallback by choices
        payload_variants = [
            {"choices": {OPTION_NAME: CHOICE_DEPOSIT}, "priceData": {"price": deposit}, "sku": f"{base_sku}-DEP"},
            {"choices": {OPTION_NAME: CHOICE_PREPAY},  "priceData": {"price": prepay, "compareAtPrice": round_price(base_price)}, "sku": f"{base_sku}-PREPAY"},
        ]

    r = api_patch(f"{WIX_API_BASE}/products/{product_id}/variants", {"variants": payload_variants})
    if not r.ok:
        raise RuntimeError(f"PATCH /products/{product_id}/variants failed {r.status_code}: {r.text}")

def try_enable_preorder(product_id):
    for payload in [
        {"product": {"isPreOrder": True}},
        {"product": {"preorderInfo": {"isPreOrder": True}}},
        {"product": {"preOrder": True}},
    ]:
        r = api_patch(f"{WIX_API_BASE}/products/{product_id}", payload)
        if r.ok:
            return True
    return False

def add_to_collections(product_id, collection_id):
    if not collection_id:
        return
    api_post(f"{WIX_API_BASE}/collections/{collection_id}/products/add", {"productIds": [product_id]}, timeout=20)

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

    r = api_post(f"{WIX_API_BASE}/products", {"product": product})
    if not r.ok:
        # se SKU non unico, alzo con messaggio pulito (sarà gestito a monte)
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
    r = api_patch(f"{WIX_API_BASE}/products/{pid}", patch)
    if not r.ok:
        raise RuntimeError(f"PATCH /products/{pid} failed {r.status_code}: {r.text}")

def main():
    if not API_KEY or not SITE_ID:
        print("[FATAL] WIX_API_KEY / WIX_SITE_ID mancanti"); sys.exit(2)

    # Collezioni
    try:
        col_map = get_collections_map()
        log("[INFO] Categorie caricate:", ", ".join(list(col_map.keys())))
    except Exception as e:
        log(f"[WARN] Lettura categorie fallita: {e}")
        col_map = {}

    # Prodotti esistenti (SKU → prodotto)
    existing_by_sku = list_all_products_by_sku()

    # CSV
    csv_path = resolve_csv_path()
    log(f"[INFO] CSV: {csv_path}")

    created = updated = 0
    with open(csv_path, "r", encoding="utf-8-sig", newline="") as fh:
        rd = csv.DictReader(fh, delimiter=";")
        required = {"nome_articolo", "prezzo_eur", "sku"}
        headers = set((rd.fieldnames or []))
        if not rd.fieldnames or not required.issubset(headers):
            miss = sorted(list(required - headers))
            print(f"[FATAL] CSV mancano colonne: {miss}"); sys.exit(2)

        for i, row in enumerate(rd, start=2):
            name = (row.get("nome_articolo") or "").strip()
            sku  = (row.get("sku") or "").strip()
            price_raw = (row.get("prezzo_eur") or "").strip()
            if not name or not sku or not price_raw:
                log(f"[SKIP] Riga {i}: dati incompleti.")
                continue
            try:
                price = float(price_raw.replace(",", "."))
            except:
                log(f"[SKIP] Riga {i}: prezzo non valido.")
                continue

            # categoria
            cat_id = None
            raw_cat = (row.get("categoria") or "").strip()
            if raw_cat and col_map:
                cat_id = col_map.get(norm(raw_cat))
                if not cat_id:
                    log(f"[WARN] Categoria '{raw_cat}' non trovata: il prodotto resterà senza categoria.")

            sku_l = sku.lower()
            existing = existing_by_sku.get(sku_l)

            try:
                if existing:
                    pid = existing.get("id")
                    patch_product_main(pid, row, price, cat_id)
                    force_options(pid)
                    ensure_manage_variants(pid)
                    patch_variant_prices_by_id(pid, price, sku)
                    try_enable_preorder(pid)
                    add_to_collections(pid, cat_id)
                    updated += 1
                    log(f"[OK] Aggiornato: {name}")
                else:
                    # provo a creare
                    try:
                        pid = create_product(row, collection_id=cat_id)
                        # nuova creazione: ricarico in cache
                        existing_by_sku[sku_l] = {"id": pid, "sku": sku}
                    except RuntimeError as e:
                        msg = str(e)
                        if "product.sku is not unique" in msg:
                            # ricarico elenco prodotti e riprovo come update
                            existing_by_sku = list_all_products_by_sku()
                            existing = existing_by_sku.get(sku_l)
                            if not existing:
                                raise
                            pid = existing.get("id")
                        else:
                            raise

                    # ora patch comune
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
