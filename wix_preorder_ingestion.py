# -*- coding: utf-8 -*-
"""
Import preordini su Wix Stores da CSV V7 (colonne minime: nome_articolo;prezzo_eur;sku).
Colonne usate se presenti: descrizione;preorder_deadline;eta;brand;categoria

Cosa fa:
- Risolve ID categorie via /collections/query.
- Se SKU esiste: patch del prodotto (nome, descrizione, brand, prezzo, categorie, ribbon/tags)
  e forzatura delle Product Options corrette.
- Se SKU non esiste: crea il prodotto con Product Options corrette.
- Imposta due varianti con prezzi:
    ANTICIPO/SALDO = 30% del prezzo
    PAGAMENTO ANTICIPATO = prezzo -5%, con compareAt al prezzo pieno
- Aggiunge riga vuota fra intestazione (DEADLINE/ETA) e corpo descrizione.
- Prova ad attivare flag preordine (best-effort).

Env richieste: WIX_API_KEY, WIX_SITE_ID
CSV: 1) argomento CLI, oppure 2) env CSV_PATH, oppure 3) percorsi noti.
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

def query_product_by_sku(sku):
    """Cerca 1 prodotto per SKU. Non blocca se la query è schizzinosa."""
    url = f"{WIX_API}/products/query"
    payloads = [
        {"query": {"filter": {"sku": {"$eq": sku}}, "paging": {"limit": 1}}},
        {"query": {"filter": {"sku": sku}, "paging": {"limit": 1}}}
    ]
    for p in payloads:
        r = requests.post(url, headers=HEADERS, data=json.dumps(p), timeout=30)
        if r.ok:
            items = r.json().get("products", [])
            return items[0] if items else None
    return None

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

# ---------- product creation / update ----------
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
    """Forza le Product Options corrette sul prodotto esistente."""
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

# ---------- variants ----------
def patch_variants(product_id, base_price, base_sku):
    """Qui il fix: choices deve essere UN OGGETTO {optionName: choiceValue}."""
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
        if not rd.fieldnames or not required.issubset(set([c.strip() for c in rd.fieldnames])):
            miss = required - set([c.strip() for c in (rd.fieldnames or [])])
            print(f"[FATAL] CSV mancano colonne: {sorted(miss)}"); sys.exit(2)

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

            cat_id = None
            cat = (row.get("categoria") or "").strip().lower()
            if cat and col_map:
                cat_id = col_map.get(cat) or next((v for k, v in col_map.items()
                         if k.replace(" ", "") == cat.replace(" ", "")), None)
                if not cat_id:
                    log(f"[WARN] Categoria '{row.get('categoria')}' non trovata.")

            existing = None
            try:
                existing = query_product_by_sku(sku)
            except Exception as e:
                log(f"[WARN] Query SKU fallita: {e}")

            try:
                if existing:
                    pid = existing["id"]
                    # patch main fields + categorie
                    patch_product_main(pid, row, price, cat_id)
                    # forzo le opzioni corrette, poi varianti
                    force_options(pid)
                    patch_variants(pid, price, sku)
                    try_enable_preorder(pid)
                    add_to_collections(pid, cat_id)
                    updated += 1
                    log(f"[OK] Aggiornato: {name}")
                else:
                    pid = create_product(row, collection_id=cat_id)
                    if not pid:
                        raise RuntimeError("ID prodotto non ricevuto.")
                    # varianti e resto
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
