#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Importa/aggiorna prodotti PREORDER su Wix partendo da CSV (v7).
Niente scraping: usiamo le colonne del file.
- Crea o aggiorna per SKU (se esiste -> PATCH, altrimenti -> POST)
- Imposta brand, categoria (collection), ribbon PREORDER, descrizione con Scadenza/ETA
- Crea opzione: PREORDER PAYMENTS OPTIONS* con 2 scelte:
    - ANTICIPO/SALDO (prezzo = 30% del prezzo di vendita)
    - PAGAMENTO ANTICIPATO (prezzo = prezzo - 5%)
- Abilita prezzi per variante (manageVariants = true) e aggiorna i prezzi delle 2 varianti
- Aggiunge il prodotto alla collection corrispondente al campo "categoria"
Note: se non troviamo la collection, logghiamo un warning e andiamo avanti.
"""

import os, sys, csv, json, time, re
import requests
from typing import Dict, Any, Optional

API_BASE = "https://www.wixapis.com/stores/v1"

# Env vars richieste
WIX_API_KEY = os.environ.get("WIX_API_KEY", "").strip()
WIX_SITE_ID = os.environ.get("WIX_SITE_ID", "").strip()
CSV_PATH = os.environ.get("CSV_PATH", "template_preordini_v7.csv")

if not WIX_API_KEY or not WIX_SITE_ID:
    print("[FATAL] Manca WIX_API_KEY o WIX_SITE_ID negli env.")
    sys.exit(2)

HEADERS = {
    "Authorization": WIX_API_KEY,        # usa esattamente il formato che ti funziona ora
    "wix-site-id": WIX_SITE_ID,
    "Content-Type": "application/json"
}

# Opzione/Varianti standard
OPTION_NAME = "PREORDER PAYMENTS OPTIONS*"
CHOICE_DEPOSIT = "ANTICIPO/SALDO"
CHOICE_UPFRONT = "PAGAMENTO ANTICIPATO"
CHOICE_DEPOSIT_DESC = "Paga 30% ora, saldo alla disponibilità"
CHOICE_UPFRONT_DESC = "Pagamento immediato con sconto 5%"

# Campi CSV attesi (v7)
CSV_COLUMNS = [
    "nome_articolo", "prezzo_eur", "sku", "brand", "categoria",
    "descrizione", "preorder_scadenza", "eta", "url_prodotto"
]

def _safe_price(p: str) -> float:
    if p is None:
        return 0.0
    s = str(p).replace("€", "").replace(" ", "").replace(",", ".")
    try:
        return round(float(s), 2)
    except Exception:
        return 0.0

def _truncate(text: str, max_len: int) -> str:
    t = text or ""
    return t if len(t) <= max_len else t[:max_len]

def _desc_html(scadenza: str, eta: str, body_txt: str) -> str:
    scad = scadenza.strip() if scadenza else "-"
    eta_ = eta.strip() if eta else "-"
    prefix = f"<p><strong>PREORDER DEADLINE:</strong> {scad}<br><strong>ETA:</strong> {eta_}</p>"
    body = (body_txt or "")
    body_lines = body.splitlines()
    body_html = "<br>".join(body_lines) if body_lines else ""
    if body_html:
        return prefix + f"<div><p>{body_html}</p></div>"
    return prefix

def _collections_map() -> Dict[str, str]:
    # Prende le collections (prime 100)
    url = f"{API_BASE}/collections/query"
    payload = {"query": {"paging": {"limit": 100}}}
    try:
        r = requests.post(url, headers=HEADERS, data=json.dumps(payload), timeout=30)
        if r.status_code >= 400:
            print(f"[WARN] Collections query: {r.status_code} {r.text}")
            return {}
        data = r.json()
        out = {}
        for c in data.get("collections", []):
            name = c.get("name", "").strip()
            cid = c.get("id")
            if name and cid:
                out[name] = cid
        if out:
            first = ", ".join(list(out.keys())[:20])
            print(f"[INFO] Collections disponibili (prime 20): {first}")
        return out
    except Exception as e:
        print(f"[WARN] Collections query exception: {e}")
        return {}

def _query_product_by_sku(sku: str) -> Optional[Dict[str, Any]]:
    url = f"{API_BASE}/products/query"
    payload = {
        "query": {
            "filter": {"sku": {"$eq": sku}},
            "paging": {"limit": 1}
        }
    }
    r = requests.post(url, headers=HEADERS, data=json.dumps(payload), timeout=30)
    if r.status_code >= 400:
        print(f"[WARN] Query SKU '{sku}': {r.status_code} {r.text}")
        return None
    items = r.json().get("products", [])
    return items[0] if items else None

def _create_product(row: Dict[str, str], base_price: float, html_desc: str) -> Optional[str]:
    name = _truncate(row.get("nome_articolo", "").strip(), 80)
    sku = row.get("sku", "").strip()
    brand = (row.get("brand") or "").strip()

    payload = {
        "product": {
            "name": name,
            "sku": sku,
            "brand": brand,
            "visible": True,
            "productType": "physical",   # fondamentale, altrimenti 400
            "ribbon": "PREORDER",
            "priceData": {"price": base_price},
            "manageVariants": True,
            "description": html_desc,
            "productOptions": [
                {
                    "name": OPTION_NAME,
                    "choices": [
                        {"value": CHOICE_DEPOSIT, "description": CHOICE_DEPOSIT_DESC},
                        {"value": CHOICE_UPFRONT, "description": CHOICE_UPFRONT_DESC}
                    ]
                }
            ],
            "tags": ["PREORDER"]
        }
    }

    url = f"{API_BASE}/products"
    r = requests.post(url, headers=HEADERS, data=json.dumps(payload), timeout=40)
    if r.status_code >= 400:
        print(f"[ERRORE] POST /products failed {r.status_code}: {r.text}")
        return None
    prod = r.json().get("product")
    return prod.get("id") if prod else None

def _patch_product(product_id: str, row: Dict[str, str], base_price: float, html_desc: str) -> bool:
    name = _truncate(row.get("nome_articolo", "").strip(), 80)
    brand = (row.get("brand") or "").strip()

    payload = {
        "product": {
            "id": product_id,
            "name": name,
            "brand": brand,
            "visible": True,
            "productType": "physical",
            "ribbon": "PREORDER",
            "priceData": {"price": base_price},
            "manageVariants": True,
            "description": html_desc,
            "productOptions": [
                {
                    "name": OPTION_NAME,
                    "choices": [
                        {"value": CHOICE_DEPOSIT, "description": CHOICE_DEPOSIT_DESC},
                        {"value": CHOICE_UPFRONT, "description": CHOICE_UPFRONT_DESC}
                    ]
                }
            ],
            "tags": ["PREORDER"]
        }
    }

    url = f"{API_BASE}/products/{product_id}"
    r = requests.patch(url, headers=HEADERS, data=json.dumps(payload), timeout=40)
    if r.status_code >= 400:
        print(f"[ERRORE] PATCH /products/{product_id} failed {r.status_code}: {r.text}")
        return False
    return True

def _patch_variant_prices(product_id: str, base_price: float) -> bool:
    # Calcolo prezzi varianti
    deposit = round(base_price * 0.30, 2)
    upfront = round(base_price * 0.95, 2)

    payload = {
        "variants": [
            {
                "choices": {OPTION_NAME: CHOICE_DEPOSIT},
                "priceData": {"price": deposit}
            },
            {
                "choices": {OPTION_NAME: CHOICE_UPFRONT},
                "priceData": {"price": upfront}
            }
        ]
    }
    url = f"{API_BASE}/products/{product_id}/variants"
    r = requests.patch(url, headers=HEADERS, data=json.dumps(payload), timeout=40)
    if r.status_code >= 400:
        print(f"[ERRORE] PATCH /products/{product_id}/variants failed {r.status_code}: {r.text}")
        return False
    return True

def _add_to_collection(product_id: str, coll_id: str) -> bool:
    url = f"{API_BASE}/collections/{coll_id}/products/add"
    payload = {"productIds": [product_id]}
    r = requests.post(url, headers=HEADERS, data=json.dumps(payload), timeout=30)
    if r.status_code >= 400:
        # Può fallire con 409 se già presente; non lo consideriamo blocco
        if r.status_code != 409:
            print(f"[WARN] Add to collection {coll_id}: {r.status_code} {r.text}")
        return False
    return True

def main() -> int:
    # Precheck banale
    try:
        rq = requests.post(f"{API_BASE}/products/query",
                           headers=HEADERS,
                           data=json.dumps({"query": {"paging": {"limit": 1}}}),
                           timeout=20)
        if rq.status_code >= 400:
            print(f"[FATAL] API non raggiungibile: {rq.status_code} {rq.text}")
            return 2
        visibili = rq.json().get("products", [])
        print(f"[PRECHECK] API ok. Prodotti visibili: {len(visibili)}")
    except Exception as e:
        print(f"[FATAL] API errore: {e}")
        return 2

    # Carico CSV
    if not os.path.exists(CSV_PATH):
        print(f"[FATAL] CSV non trovato: {CSV_PATH}")
        return 2

    # Mappa collections
    coll_map = _collections_map()

    creati_o_aggiornati = 0

    with open(CSV_PATH, "r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f, delimiter=";")
        # Validazione colonne
        cols = [c.strip() for c in reader.fieldnames or []]
        missing = [c for c in CSV_COLUMNS if c not in cols]
        if missing:
            print(f"[FATAL] CSV mancano colonne: {missing}")
            return 2

        riga_idx = 1
        for row in reader:
            riga_idx += 1
            name = (row.get("nome_articolo") or "").strip()
            price = _safe_price(row.get("prezzo_eur"))
            sku = (row.get("sku") or "").strip()

            if not name or not price or not sku:
                print(f"[SKIP] Riga {riga_idx}: nome_articolo/prezzo_eur/sku mancanti.")
                continue

            print(f"[WORK] Riga {riga_idx}: {name}")

            scadenza = row.get("preorder_scadenza") or ""
            eta = row.get("eta") or ""
            desc_txt = row.get("descrizione") or ""
            html_desc = _desc_html(scadenza, eta, desc_txt)

            # cerca per SKU
            existing = _query_product_by_sku(sku)

            product_id = None
            if existing:
                product_id = existing.get("id")
                ok = _patch_product(product_id, row, price, html_desc)
                if not ok:
                    print(f"[ERRORE] PATCH fallita per SKU {sku}")
                    continue
            else:
                product_id = _create_product(row, price, html_desc)
                if not product_id:
                    print(f"[ERRORE] Creazione fallita per SKU {sku}")
                    continue

            # Varianti: prezzi
            if not _patch_variant_prices(product_id, price):
                print(f"[WARN] Aggiornamento prezzi varianti fallito per {product_id}")

            # Collection
            categoria = (row.get("categoria") or "").strip()
            if categoria and coll_map.get(categoria):
                _add_to_collection(product_id, coll_map[categoria])
            elif categoria:
                print(f"[WARN] Collection '{categoria}' non trovata, il prodotto non sarà categorizzato.")

            creati_o_aggiornati += 1

    if creati_o_aggiornati == 0:
        print("[ERRORE] Nessun prodotto creato/aggiornato.")
        return 2

    print(f"[OK] Prodotti creati/aggiornati: {creati_o_aggiornati}")
    return 0

if __name__ == "__main__":
    sys.exit(main())
