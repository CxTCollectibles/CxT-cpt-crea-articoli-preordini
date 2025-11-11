# -*- coding: utf-8 -*-
"""
Ingestione preordini su Wix Stores partendo dal CSV V7 dell'utente.
- Usa campi dal CSV: nome_articolo, prezzo_eur, sku, descrizione, preorder_deadline, eta, brand, categoria
- Categorie mappate per nome -> ID (POST /collections/query)
- Opzione + Varianti:
  * Opzione: PREORDER PAYMENTS OPTIONS*
  * Scelte:  ANTICIPO/SALDO (30% del prezzo)
             PAGAMENTO ANTICIPATO (prezzo -5%, compareAt = prezzo pieno)
- Descrizione: HEAD (PREORDER DEADLINE, ETA) + riga vuota + corpo
- Brand impostato se presente
- Tentativo di abilitare flag preordine (best-effort)

Richiede env:
  WIX_API_KEY
  WIX_SITE_ID
Facoltativo:
  CSV_PATH  -> percorso al CSV; altrimenti auto-rilevamento
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

def round_price(x):
    return float(f"{x:.2f}")

def resolve_csv_path():
    # Priorità: argv, env, poi percorsi comuni
    candidates = []
    if len(sys.argv) > 1 and sys.argv[1].strip():
        candidates.append(sys.argv[1].strip())
    if os.environ.get("CSV_PATH"):
        candidates.append(os.environ["CSV_PATH"].strip())
    candidates += [
        "template_preordini_v7.csv",
        "input/template_preordini_v7.csv",
        "data/template_preordini_v7.csv",
        "csv/template_preordini_v7.csv"
    ]
    for p in candidates:
        if p and os.path.exists(p):
            return p
    raise FileNotFoundError(f"CSV non trovato. Provati: {candidates}")

def get_collections_map():
    """
    Ritorna {nome_lower: id} usando POST /collections/query
    Gestisce paging (max 100 per pagina).
    """
    url = f"{WIX_API}/collections/query"
    collections = {}
    cursor = None
    while True:
        payload = {"query": {"paging": {"limit": 100}}}
        if cursor:
            payload["query"]["paging"]["cursor"] = cursor
        r = requests.post(url, headers=HEADERS, data=json.dumps(payload), timeout=30)
        if not r.ok:
            raise requests.HTTPError(f"{r.status_code} {r.text}")
        data = r.json()
        for c in data.get("collections", []):
            name = (c.get("name") or "").strip()
            cid = c.get("id")
            if name and cid:
                collections[name.lower()] = cid
        cursor = data.get("paging", {}).get("nextCursor")
        if not cursor:
            break
    return collections

def query_product_by_sku(sku):
    url = f"{WIX_API}/products/query"
    payload = {"query": {"filter": {"sku": {"$eq": sku}}, "paging": {"limit": 50}}}
    r = requests.post(url, headers=HEADERS, data=json.dumps(payload), timeout=30)
    r.raise_for_status()
    items = r.json().get("products", [])
    return items[0] if items else None

def build_description(row):
    descr_raw = (row.get("descrizione") or "").strip()
    dl = (row.get("preorder_deadline") or "").strip()
    eta = (row.get("eta") or "").strip()

    head_parts = []
    if dl:  head_parts.append(f"<strong>PREORDER DEADLINE:</strong> {dl}")
    if eta: head_parts.append(f"<strong>ETA:</strong> {eta}")
    head_html = "<br>".join(head_parts) if head_parts else ""

    body_html = (descr_raw.replace("\n", "<br>") if descr_raw else "").strip()

    if head_html and body_html:
        return f"<p>{head_html}</p><p><br></p><p>{body_html}</p>"
    elif head_html:
        return f"<p>{head_html}</p>"
    elif body_html:
        return f"<p>{body_html}</p>"
    return ""

def create_product(row, collection_id=None):
    name = (row.get("nome_articolo") or "").strip()
    if len(name) > 80:
        name = name[:80]
    price = float(str(row.get("prezzo_eur") or "0").replace(",", "."))
    sku = (row.get("sku") or "").strip()
    brand = (row.get("brand") or "").strip()

    product_obj = {
        "name": name,
        "sku": sku,
        "productType": "PHYSICAL",
        "priceData": {"price": round_price(price)},
        "description": build_description(row),
        "brand": brand if brand else None,
        "ribbon": "PREORDER",
        "tags": ["PREORDER"],
        "collectionIds": [collection_id] if collection_id else [],
        "productOptions": [{
            "name": "PREORDER PAYMENTS OPTIONS*",
            "choices": [
                {"value": "ANTICIPO/SALDO"},
                {"value": "PAGAMENTO ANTICIPATO"}
            ]
        }]
    }
    # ripulisci None e liste vuote
    product_obj = {k: v for k, v in product_obj.items() if v not in (None, "", [])}

    url = f"{WIX_API}/products"
    r = requests.post(url, headers=HEADERS, data=json.dumps({"product": product_obj}), timeout=60)
    if not r.ok:
        raise RuntimeError(f"POST /products failed {r.status_code}: {r.text}")
    return r.json().get("product", {}).get("id")

def patch_variants(product_id, base_price, base_sku):
    deposit = round_price(base_price * 0.30)
    prepay  = round_price(base_price * 0.95)
    url = f"{WIX_API}/products/{product_id}/variants"
    payload = {
        "variants": [
            {
                "choices": [{"title": "PREORDER PAYMENTS OPTIONS*", "description": "ANTICIPO/SALDO"}],
                "priceData": {"price": deposit},
                "sku": f"{base_sku}-DEP"
            },
            {
                "choices": [{"title": "PREORDER PAYMENTS OPTIONS*", "description": "PAGAMENTO ANTICIPATO"}],
                "priceData": {"price": prepay, "compareAtPrice": round_price(base_price)},
                "sku": f"{base_sku}-PREPAY"
            }
        ]
    }
    r = requests.patch(url, headers=HEADERS, data=json.dumps(payload), timeout=60)
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
        r = requests.patch(url, headers=HEADERS, data=json.dumps(p), timeout=20)
        if r.ok:
            return True
    return False

def add_to_collections(product_id, collection_id):
    if not collection_id:
        return
    url = f"{WIX_API}/collections/{collection_id}/products/add"
    payload = {"productIds": [product_id]}
    r = requests.post(url, headers=HEADERS, data=json.dumps(payload), timeout=30)
    if not r.ok:
        log(f"[WARN] add_to_collections ignorato: {r.status_code}")

def main():
    if not API_KEY or not SITE_ID:
        print("[FATAL] WIX_API_KEY / WIX_SITE_ID mancanti")
        sys.exit(2)

    log("[PRECHECK] API ok. Inizio…")
    # Categorie
    try:
        col_map = get_collections_map()
        if col_map:
            log("[INFO] Categorie caricate:", ", ".join(list(col_map.keys())[:20]))
    except Exception as e:
        log(f"[WARN] Lettura categorie fallita: {e}")
        col_map = {}

    # CSV
    csv_path = resolve_csv_path()
    log(f"[INFO] CSV: {csv_path}")

    created = updated = 0
    with open(csv_path, "r", encoding="utf-8-sig", newline="") as fh:
        rd = csv.DictReader(fh, delimiter=";")
        required = {"nome_articolo", "prezzo_eur", "sku"}
        missing = required - set([c.strip() for c in (rd.fieldnames or [])])
        if missing:
            print(f"[FATAL] CSV mancano colonne: {sorted(missing)}")
            sys.exit(2)

        for i, row in enumerate(rd, start=2):
            name = (row.get("nome_articolo") or "").strip()
            sku  = (row.get("sku") or "").strip()
            price_raw = row.get("prezzo_eur")
            if not name or not sku or not price_raw:
                log(f"[SKIP] Riga {i}: dati incompleti.")
                continue
            try:
                price = float(str(price_raw).replace(",", "."))
            except:
                log(f"[SKIP] Riga {i}: prezzo non valido.")
                continue

            cat_id = None
            cat_name = (row.get("categoria") or "").strip().lower()
            if cat_name and col_map:
                cat_id = col_map.get(cat_name)
                if not cat_id:
                    # match senza spazi
                    cat_id = next((v for k, v in col_map.items()
                                   if k.replace(" ", "") == cat_name.replace(" ", "")), None)
                    if not cat_id:
                        log(f"[WARN] Categoria '{row.get('categoria')}' non trovata.")

            try:
                existing = query_product_by_sku(sku)
            except Exception as e:
                log(f"[WARN] Query SKU fallita: {e}")
                existing = None

            try:
                if existing:
                    pid = existing["id"]
                    patch = {
                        "product": {
                            "id": pid,
                            "name": (name[:80]),
                            "description": build_description(row),
                            "brand": (row.get("brand") or "").strip() or None,
                            "priceData": {"price": round_price(price)},
                            "ribbon": "PREORDER",
                            "tags": ["PREORDER"]
                        }
                    }
                    if cat_id:
                        patch["product"]["collectionIds"] = [cat_id]

                    r = requests.patch(f"{WIX_API}/products/{pid}", headers=HEADERS,
                                       data=json.dumps(patch), timeout=60)
                    if not r.ok:
                        raise RuntimeError(f"PATCH /products/{pid} failed {r.status_code}: {r.text}")

                    patch_variants(pid, price, sku)
                    try_enable_preorder(pid)
                    add_to_collections(pid, cat_id)
                    updated += 1
                    log(f"[OK] Aggiornato: {name}")
                else:
                    pid = create_product(row, collection_id=cat_id)
                    if not pid:
                        raise RuntimeError("ID prodotto non ricevuto.")
                    patch_variants(pid, price, sku)
                    try_enable_preorder(pid)
                    add_to_collections(pid, cat_id)
                    created += 1
                    log(f"[OK] Creato: {name}")
            except Exception as e:
                log(f"[ERRORE] Riga {i} '{name}': {e}")

    if not created and not updated:
        print("[ERRORE] Nessun prodotto creato/aggiornato.")
        sys.exit(2)
    print(f"[DONE] Creati: {created}, Aggiornati: {updated}")
    sys.exit(0)

if __name__ == "__main__":
    main()
