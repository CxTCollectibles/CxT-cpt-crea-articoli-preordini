# -*- coding: utf-8 -*-
"""
Ingestione preordini su Wix Stores partendo dal CSV V7 dell'utente.
- Nessun scraping: usa i campi già presenti nel CSV (nome, sku, prezzo, descrizione, ecc).
- Categorie mappate per nome -> ID e assegnate in creazione.
- Opzioni e varianti di preordine impostate e prezzate:
  * Opzione: PREORDER PAYMENTS OPTIONS*
  * Scelte/varianti: ANTICIPO/SALDO (30% del prezzo) e PAGAMENTO ANTICIPATO (5% sconto)
- Descrizione: HEAD con PREORDER DEADLINE e ETA, una riga vuota, poi testo (traduzione eventuale già nel CSV).
- Brand impostato.
- Tentativo di accendere il flag Preordine (se proprietà supportata).

Richiede:
  WIX_API_KEY  -> token API
  WIX_SITE_ID  -> site id
CSV: utf-8-sig; delimitatore ';'
Colonne minime: nome_articolo, prezzo_eur, sku
Colonne opzionali ma caldamente consigliate: descrizione, preorder_deadline, eta, brand, categoria
"""

import os, csv, sys, json, math, time
import requests

WIX_API = "https://www.wixapis.com/stores/v1"
API_KEY = os.environ.get("WIX_API_KEY", "").strip()
SITE_ID = os.environ.get("WIX_SITE_ID", "").strip()

HEADERS = {
    "Authorization": API_KEY,
    "wix-site-id": SITE_ID,
    "Content-Type": "application/json"
}

CSV_PATH = sys.argv[1] if len(sys.argv) > 1 else "template_preordini_v7.csv"

# ---------- Helpers ----------
def log(*a): 
    print(*a, flush=True)

def round_price(x):
    # prezzi come 2 decimali, arrotondamento "commerciale"
    return float(f"{x:.2f}")

def get_collections_map():
    """ Ritorna {nome_normalizzato: id} delle prime 100 collezioni. """
    url = f"{WIX_API}/collections"
    # v1 supporta paging via nextCursor, qui basta 100
    params = {"limit": 100}
    r = requests.get(url, headers=HEADERS, params=params, timeout=30)
    r.raise_for_status()
    data = r.json()
    out = {}
    for c in data.get("collections", []):
        name = (c.get("name") or "").strip()
        cid = c.get("id")
        if name and cid:
            out[name.lower()] = cid
    return out

def query_product_by_sku(sku):
    """ Cerca prodotto per SKU. Ritorna il primo product (dict) o None. """
    url = f"{WIX_API}/products/query"
    payload = {
        "query": {
            "filter": {"sku": {"$eq": sku}},
            "paging": {"limit": 50}
        }
    }
    r = requests.post(url, headers=HEADERS, data=json.dumps(payload), timeout=30)
    r.raise_for_status()
    items = r.json().get("products", [])
    return items[0] if items else None

def create_product(row, collection_id=None):
    """ Crea prodotto base con opzione e 2 scelte. Ritorna productId. """
    name = (row.get("nome_articolo") or "").strip()
    price = float(str(row.get("prezzo_eur") or "0").replace(",", "."))
    sku = (row.get("sku") or "").strip()
    brand = (row.get("brand") or "").strip()

    # tronca nome a 80 char come da vincolo API
    if len(name) > 80:
        name = name[:80]

    descr_raw = (row.get("descrizione") or "").strip()
    dl = (row.get("preorder_deadline") or "").strip()
    eta = (row.get("eta") or "").strip()

    head_parts = []
    if dl:
        head_parts.append(f"<strong>PREORDER DEADLINE:</strong> {dl}")
    if eta:
        head_parts.append(f"<strong>ETA:</strong> {eta}")
    head_html = "<br>".join(head_parts) if head_parts else ""
    body_html = (descr_raw.replace("\n", "<br>") if descr_raw else "").strip()

    # riga vuota fra head e corpo se entrambi presenti
    if head_html and body_html:
        description_html = f"<p>{head_html}</p><p><br></p><p>{body_html}</p>"
    elif head_html:
        description_html = f"<p>{head_html}</p>"
    else:
        description_html = f"<p>{body_html}</p>" if body_html else ""

    product_obj = {
        "name": name,
        "sku": sku,
        "productType": "PHYSICAL",
        "priceData": {"price": round_price(price)},
        "description": description_html,
        "brand": brand if brand else None,
        # Etichetta "PREORDER" e tag per sicurezza
        "ribbon": "PREORDER",
        "tags": ["PREORDER"],
        # Categorie in creazione (se trovata)
        "collectionIds": [collection_id] if collection_id else [],
        # Opzione con le 2 scelte necessarie
        "productOptions": [
            {
                "name": "PREORDER PAYMENTS OPTIONS*",
                "choices": [
                    {"value": "ANTICIPO/SALDO"},
                    {"value": "PAGAMENTO ANTICIPATO"}
                ]
            }
        ],
    }

    # pulizia None
    product_obj = {k: v for k, v in product_obj.items() if v not in (None, [], "")}

    url = f"{WIX_API}/products"
    r = requests.post(url, headers=HEADERS, data=json.dumps({"product": product_obj}), timeout=60)
    if not r.ok:
        raise RuntimeError(f"POST /products failed {r.status_code}: {r.text}")
    return r.json().get("product", {}).get("id")

def patch_variants(product_id, base_price, base_sku):
    """
    Aggiorna le varianti generate dall'opzione:
    - ANTICIPO/SALDO       -> 30% del prezzo
    - PAGAMENTO ANTICIPATO -> prezzo scontato 5%, compareAt = prezzo pieno
    """
    deposit = round_price(base_price * 0.30)
    prepay  = round_price(base_price * 0.95)

    url = f"{WIX_API}/products/{product_id}/variants"
    # Struttura attesa da Wix: choices con title = nome opzione, description = valore scelta
    variants_payload = {
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
    r = requests.patch(url, headers=HEADERS, data=json.dumps(variants_payload), timeout=60)
    if not r.ok:
        raise RuntimeError(f"PATCH /products/{product_id}/variants failed {r.status_code}: {r.text}")

def try_enable_preorder(product_id):
    """
    Non tutte le versioni espongono la proprietà; proviamo in modo resiliente.
    Se fallisce, non blocchiamo il flusso.
    """
    url = f"{WIX_API}/products/{product_id}"
    # tentativi comuni osservati nelle API
    candidates = [
        {"product": {"isPreOrder": True}},
        {"product": {"preorderInfo": {"isPreOrder": True}}},
        {"product": {"preOrder": True}},
    ]
    for payload in candidates:
        r = requests.patch(url, headers=HEADERS, data=json.dumps(payload), timeout=20)
        if r.ok:
            return True
    return False

def add_to_collections(product_id, collection_id):
    """ fallback nel caso collectionIds in creazione non andasse a buon fine """
    if not collection_id:
        return
    url = f"{WIX_API}/collections/{collection_id}/products/add"
    payload = {"productIds": [product_id]}
    r = requests.post(url, headers=HEADERS, data=json.dumps(payload), timeout=30)
    # Non alzare: su alcuni tenant l'endpoint è ristretto. Log e avanti.
    if not r.ok:
        log(f"[WARN] add_to_collections 404/403 ignorato: {r.status_code}")

# ---------- Main ----------
def main():
    if not API_KEY or not SITE_ID:
        print("[FATAL] Variabili d'ambiente mancanti: WIX_API_KEY / WIX_SITE_ID")
        sys.exit(2)

    log("[PRECHECK] API ok. Inizio…")
    try:
        col_map = get_collections_map()
        if col_map:
            log("[INFO] Categorie disponibili:", ", ".join(list(col_map.keys())[:20]))
    except Exception as e:
        log(f"[WARN] Lettura categorie fallita: {e}")
        col_map = {}

    created, updated = 0, 0

    with open(CSV_PATH, "r", encoding="utf-8-sig", newline="") as fh:
        rd = csv.DictReader(fh, delimiter=";")
        required = {"nome_articolo", "prezzo_eur", "sku"}
        missing = required - set([c.strip() for c in rd.fieldnames or []])
        if missing:
            print(f"[FATAL] CSV mancano colonne: {sorted(missing)}")
            sys.exit(2)

        for i, row in enumerate(rd, start=2):
            name = (row.get("nome_articolo") or "").strip()
            sku  = (row.get("sku") or "").strip()
            if not name or not sku or not row.get("prezzo_eur"):
                log(f"[SKIP] Riga {i}: dati incompleti.")
                continue

            try:
                price = float(str(row["prezzo_eur"]).replace(",", "."))
            except:
                log(f"[SKIP] Riga {i}: prezzo non valido.")
                continue

            # Categoria (per nome, case-insensitive)
            cat_name = (row.get("categoria") or "").strip().lower()
            cat_id = col_map.get(cat_name) if cat_name else None
            if not cat_id and cat_name:
                # prova match "lasco" rimuovendo spazi multipli
                cat_id = next((v for k, v in col_map.items() if k.replace(" ", "") == cat_name.replace(" ", "")), None)
                if not cat_id:
                    log(f"[WARN] Categoria '{row.get('categoria')}' non trovata, proseguo senza.")

            # esiste già per SKU?
            existing = None
            try:
                existing = query_product_by_sku(sku)
            except Exception as e:
                log(f"[WARN] Query SKU fallita: {e}")

            try:
                if existing:
                    pid = existing["id"]
                    # aggiorna base (nome, brand, descrizione, prezzo pieno, ribbon, tags, e eventualmente collectionIds)
                    descr_raw = (row.get("descrizione") or "").strip()
                    dl = (row.get("preorder_deadline") or "").strip()
                    eta = (row.get("eta") or "").strip()

                    head = []
                    if dl: head.append(f"<strong>PREORDER DEADLINE:</strong> {dl}")
                    if eta: head.append(f"<strong>ETA:</strong> {eta}")
                    head_html = "<br>".join(head)
                    body_html = (descr_raw.replace("\n", "<br>") if descr_raw else "").strip()

                    if head_html and body_html:
                        description_html = f"<p>{head_html}</p><p><br></p><p>{body_html}</p>"
                    elif head_html:
                        description_html = f"<p>{head_html}</p>"
                    else:
                        description_html = f"<p>{body_html}</p>" if body_html else ""

                    patch = {
                        "product": {
                            "id": pid,
                            "name": name[:80],
                            "description": description_html,
                            "brand": (row.get("brand") or "").strip() or None,
                            "priceData": {"price": round_price(price)},
                            "ribbon": "PREORDER",
                            "tags": ["PREORDER"]
                        }
                    }
                    # se abbiamo una categoria e non è già presente, aggiungiamola
                    if cat_id:
                        patch["product"]["collectionIds"] = [cat_id]

                    urlp = f"{WIX_API}/products/{pid}"
                    r = requests.patch(urlp, headers=HEADERS, data=json.dumps(patch), timeout=60)
                    if not r.ok:
                        raise RuntimeError(f"PATCH /products/{pid} failed {r.status_code}: {r.text}")

                    # opzioni/varianti
                    patch_variants(pid, price, sku)
                    try_enable_preorder(pid)
                    add_to_collections(pid, cat_id)
                    updated += 1
                    log(f"[OK] Aggiornato: {name}")
                else:
                    # crea
                    pid = create_product(row, collection_id=cat_id)
                    if not pid:
                        raise RuntimeError("ID prodotto non ricevuto in creazione.")
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
