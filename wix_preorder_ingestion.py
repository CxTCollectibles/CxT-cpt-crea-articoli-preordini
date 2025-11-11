# -*- coding: utf-8 -*-
"""
Import preordini su Wix Stores da CSV V7 (colonne minime: nome_articolo;prezzo_eur;sku).
Colonne opzionali usate se presenti: descrizione;preorder_deadline;eta;brand;categoria

Comportamento:
- Risolve ID categorie via /collections/query.
- Cerca per SKU (3 strategie + scan paginato). Se esiste: PATCH. Se non esiste: POST.
- Se il POST fallisce con 'product.sku is not unique', ri-cerca e passa a PATCH.
- Forza sempre le Product Options:
    - PREORDER PAYMENTS OPTIONS*
        - ANTICIPO/SALDO  (= 30% del prezzo base)
        - PAGAMENTO ANTICIPATO (= 95% del prezzo base, compareAtPrice = prezzo base)
- Descrizione con intestazione DEADLINE/ETA, poi riga vuota, poi corpo.
- Aggiunge alla categoria (collection) se trovata.
- Prova ad attivare il preorder.

Richiede:
  - Env: WIX_API_KEY, WIX_SITE_ID
  - CSV path: argomento CLI oppure env CSV_PATH oppure file noti.
"""

import os, sys, csv, json
import requests

WIX_API = "https://www.wixapis.com/stores/v1"
API_KEY = os.environ.get("WIX_API_KEY", "").strip()
SITE_ID = os.environ.get("WIX_SITE_ID", "").strip()

HEADERS = {
    "Authorization": API_KEY,
    "wix-site-id": SITE_ID,
    "Content-Type": "application/json"
}

def log(*a): print(*a, flush=True)
def round_price(x): return float(f"{x:.2f}")

# ---------- utils ----------
def resolve_csv_path():
    cand = []
    if len(sys.argv) > 1 and sys.argv[1].strip():
        cand.append(sys.argv[1].strip())
    if os.environ.get("CSV_PATH"):
        cand.append(os.environ["CSV_PATH"].strip())
    cand += [
        "template_preordini_v7.csv",
        "input/template_preordini_v7.csv",
        "data/template_preordini_v7.csv",
        "csv/template_preordini_v7.csv"
    ]
    for p in cand:
        if p and os.path.exists(p):
            return p
    raise FileNotFoundError(f"CSV non trovato. Provati: {cand}")

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
                out[name.lower()] = cid
        cursor = data.get("paging", {}).get("nextCursor")
        if not cursor:
            break
    return out

# ---------- product lookup by SKU ----------
def query_product_by_sku_v1(sku):
    # filtro "classico"
    url = f"{WIX_API}/products/query"
    payload = {"query": {"filter": {"sku": {"$eq": sku}}, "paging": {"limit": 1}}}
    r = requests.post(url, headers=HEADERS, data=json.dumps(payload), timeout=20)
    if r.ok:
        items = r.json().get("products", [])
        return items[0] if items else None
    return None

def query_product_by_sku_v2(sku):
    # alcuni ambienti accettano filter semplice
    url = f"{WIX_API}/products/query"
    payload = {"query": {"filter": {"sku": sku}, "paging": {"limit": 1}}}
    r = requests.post(url, headers=HEADERS, data=json.dumps(payload), timeout=20)
    if r.ok:
        items = r.json().get("products", [])
        return items[0] if items else None
    return None

def scan_products_find_sku(sku):
    # fallback robusto: pagina tutta la lista e confronta client-side
    url = f"{WIX_API}/products/query"
    cursor = None
    sku_norm = (sku or "").strip().lower()
    while True:
        payload = {"query": {"paging": {"limit": 100}}}
        if cursor:
            payload["query"]["paging"]["cursor"] = cursor
        r = requests.post(url, headers=HEADERS, data=json.dumps(payload), timeout=30)
        if not r.ok:
            break
        data = r.json()
        for p in data.get("products", []):
            if (p.get("sku") or "").strip().lower() == sku_norm:
                return p
        cursor = data.get("paging", {}).get("nextCursor")
        if not cursor:
            break
    return None

def find_product_by_sku(sku):
    # prova v1 -> v2 -> scan
    try:
        p = query_product_by_sku_v1(sku)
        if p: return p
    except Exception: pass
    try:
        p = query_product_by_sku_v2(sku)
        if p: return p
    except Exception: pass
    try:
        p = scan_products_find_sku(sku)
        if p: return p
    except Exception: pass
    return None

# ---------- description ----------
def build_description(row):
    descr_raw = (row.get("descrizione") or "").strip()
    dl  = (row.get("preorder_deadline") or "").strip()
    eta = (row.get("eta") or "").strip()

    head = []
    if dl:  head.append(f"<strong>PREORDER DEADLINE:</strong> {dl}")
    if eta: head.append(f"<strong>ETA:</strong> {eta}")
    head_html = "<br>".join(head) if head else ""

    body_html = (descr_raw.replace("\n", "<br>") if descr_raw else "").strip()

    if head_html and body_html:
        return f"<p>{head_html}</p><p><br></p><p>{body_html}</p>"
    elif head_html:
        return f"<p>{head_html}</p>"
    elif body_html:
        return f"<p>{body_html}</p>"
    return ""

# ---------- constants for options ----------
OPTION_NAME = "PREORDER PAYMENTS OPTIONS*"
CHOICE_DEPOSIT = "ANTICIPO/SALDO"
CHOICE_PREPAY  = "PAGAMENTO ANTICIPATO"

def options_payload():
    return [{
        "name": OPTION_NAME,
        "choices": [
            {"value": CHOICE_DEPOSIT, "description": CHOICE_DEPOSIT},
            {"value": CHOICE_PREPAY,  "description": CHOICE_PREPAY}
        ]
    }]

# ---------- create / patch ----------
def create_product(row, collection_id=None):
    name = (row.get("nome_articolo") or "").strip()
    if len(name) > 80: name = name[:80]
    price = float(str(row.get("prezzo_eur") or "0").replace(",", "."))
    sku   = (row.get("sku") or "").strip()
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
        "collectionIds": [collection_id] if collection_id else [],
        "productOptions": options_payload()
    }
    # pulizia
    product = {k: v for k, v in product.items() if v not in (None, "", [])}

    r = requests.post(f"{WIX_API}/products", headers=HEADERS,
                      data=json.dumps({"product": product}), timeout=60)
    if not r.ok:
        raise RuntimeError(f"POST /products failed {r.status_code}: {r.text}")
    return r.json().get("product", {}).get("id")

def force_options(product_id):
    patch = {"product": {"id": product_id, "productOptions": options_payload()}}
    r = requests.patch(f"{WIX_API}/products/{product_id}", headers=HEADERS,
                       data=json.dumps(patch), timeout=30)
    if not r.ok:
        raise RuntimeError(f"PATCH options failed {r.status_code}: {r.text}")

def patch_product_main(pid, row, price, collection_id):
    name = (row.get("nome_articolo") or "").strip()
    if len(name) > 80: name = name[:80]
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

def patch_variants(product_id, base_price, base_sku):
    deposit = round_price(base_price * 0.30)
    prepay  = round_price(base_price * 0.95)

    payload = {
        "variants": [
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
    }
    r = requests.patch(f"{WIX_API}/products/{product_id}/variants",
                       headers=HEADERS, data=json.dumps(payload), timeout=60)
    if not r.ok:
        raise RuntimeError(f"PATCH /products/{product_id}/variants failed {r.status_code}: {r.text}")

def try_enable_preorder(product_id):
    url = f"{WIX_API}/products/{product_id}"
    candidates = [
        {"product": {"isPreOrder": True}},
        {"product": {"preorderInfo": {"isPreOrder": True}}},
        {"product": {"preOrder": True}},
    ]
    for p in candidates:
        r = requests.patch(url, headers=HEADERS, data=json.dumps(p), timeout=15)
        if r.ok: return True
    return False

def add_to_collections(product_id, collection_id):
    if not collection_id: return
    url = f"{WIX_API}/collections/{collection_id}/products/add"
    payload = {"productIds": [product_id]}
    requests.post(url, headers=HEADERS, data=json.dumps(payload), timeout=20)

# ---------- main ----------
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
                log(f"[SKIP] Riga {i}: dati incompleti."); continue
            try:
                price = float(str(price_raw).replace(",", "."))
            except:
                log(f"[SKIP] Riga {i}: prezzo non valido."); continue

            # categoria
            cat_id = None
            raw_cat = (row.get("categoria") or "").strip().lower()
            if raw_cat and col_map:
                cat_id = col_map.get(raw_cat) or next(
                    (v for k, v in col_map.items()
                     if k.replace(" ", "") == raw_cat.replace(" ", "")), None
                )
                if not cat_id:
                    log(f"[WARN] Categoria '{row.get('categoria')}' non trovata.")

            # lookup SKU
            existing = find_product_by_sku(sku)

            def do_update(pid):
                patch_product_main(pid, row, price, cat_id)
                force_options(pid)
                patch_variants(pid, price, sku)
                try_enable_preorder(pid)
                add_to_collections(pid, cat_id)

            try:
                if existing:
                    pid = existing["id"]
                    do_update(pid)
                    updated += 1
                    log(f"[OK] Aggiornato: {name}")
                else:
                    try:
                        pid = create_product(row, collection_id=cat_id)
                    except RuntimeError as e:
                        msg = str(e)
                        if "sku is not unique" in msg.lower():
                            # qualcuno l’ha già creato: trova e patcha
                            found = find_product_by_sku(sku)
                            if not found:
                                raise
                            pid = found["id"]
                            do_update(pid)
                            updated += 1
                            log(f"[OK] Aggiornato (da duplicate SKU): {name}")
                        else:
                            raise
                    else:
                        patch_variants(pid, price, sku)
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
