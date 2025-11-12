#!/usr/bin/env python3
import csv
import html
import json
import os
import re
import sys
from typing import Dict, Optional, Tuple
import requests

BASE = "https://www.wixapis.com"

API_KEY = os.environ.get("WIX_API_KEY", "").strip()
SITE_ID = os.environ.get("WIX_SITE_ID", "").strip()

HEADERS = {
    "Authorization": API_KEY,
    "wix-site-id": SITE_ID,
    "Content-Type": "application/json"
}

# Mappa nome categoria (minuscolo) -> id (riempita a runtime)
CATEGORIES: Dict[str, str] = {}

# Nomi esatti delle due opzioni e della dimensione opzione
OPT_TITLE = "PREORDER PAYMENTS OPTIONS*"
CHOICE_AS = "ANTICIPO/SALDO"
CHOICE_PA = "PAGAMENTO ANTICIPATO"

def die(msg: str, code: int = 2):
    print(f"[ERRORE] {msg}")
    sys.exit(code)

def req(method: str, path: str, *, json_body=None, params=None, ok=(200,)):
    url = f"{BASE}{path}"
    r = requests.request(method, url, headers=HEADERS, json=json_body, params=params, timeout=30)
    if r.status_code not in ok:
        raise requests.HTTPError(f"{method} {path} failed {r.status_code}: {r.text}")
    return r

def sanitize_name(name: str) -> str:
    name = (name or "").strip()
    if len(name) > 80:
        print("[WARN] Nome > 80 caratteri, troncato.")
    return name[:80]

def parse_price(eur: str) -> float:
    s = (eur or "").strip().replace("€","").replace(",",".")
    return round(float(s), 2)

def build_description(deadline: str, eta: str, body: str) -> str:
    deadline = (deadline or "").strip()
    eta = (eta or "").strip()
    body = (body or "").strip()
    parts = []
    if deadline or eta:
        parts.append(
            f"<p><strong>PREORDER DEADLINE:</strong> {html.escape(deadline) if deadline else '-'}"
            f" | <strong>ETA:</strong> {html.escape(eta) if eta else '-'}</p>"
        )
        parts.append("<p>&nbsp;</p>")  # riga vuota
    if body:
        safe = html.escape(body)
        safe = safe.replace("\r\n","<br>").replace("\n","<br>")
        parts.append(f"<div><p>{safe}</p></div>")
    return "".join(parts) if parts else ""

def load_categories():
    global CATEGORIES
    try:
        r = req("GET", "/stores/v1/collections", params={"limit": 100})
        data = r.json()
        cats = data.get("collections", [])
        CATEGORIES = { (c.get("name","").strip().lower()): c.get("id") for c in cats if c.get("id") }
        if CATEGORIES:
            print("[INFO] Categorie caricate:", ", ".join(sorted(CATEGORIES.keys())))
        else:
            print("[WARN] Nessuna categoria recuperata.")
    except Exception as e:
        print(f"[WARN] Lettura categorie fallita: {e}")

def add_to_category(prod_id: str, category_name: str):
    if not category_name:
        return
    cat_id = CATEGORIES.get(category_name.strip().lower())
    if not cat_id:
        print(f"[WARN] Categoria '{category_name}' non trovata, skip.")
        return
    try:
        payload = {"productIds": [prod_id]}
        req("POST", f"/stores/v1/collections/{cat_id}/products/add", json_body=payload, ok=(200,204))
        print(f"[INFO] Assegnato a categoria '{category_name}'")
    except Exception as e:
        print(f"[WARN] Assegnazione categoria '{category_name}' fallita: {e}")

def query_product_by_sku(sku: str) -> Optional[Tuple[str, str]]:
    """
    Ritorna (productId, revision) se trovato via API v3 (usiamo reader per id e poi get v3 per revision)
    """
    try:
        # 1) cerca per SKU con endpoint reader (filtri $eq). Docs: stores-reader/v1/products/query
        # https://dev.wix.com/docs/rest/business-solutions/stores/products/filter-sort#sku
        payload = {"query": {"filter": {"sku": {"$eq": sku}}, "paging": {"limit": 50}}}
        r = req("POST", "/stores-reader/v1/products/query", json_body=payload)
        items = r.json().get("products", [])
        if not items:
            return None
        pid = items[0].get("id")
        if not pid:
            return None
        # 2) leggi prodotto v3 per prendere revision (serve per update v3)
        r2 = req("GET", f"/stores/v3/products/{pid}")
        prod = r2.json().get("product", {})
        return pid, prod.get("revision", "1")
    except Exception:
        return None

def create_or_update(row: Dict[str, str]):
    name = sanitize_name(row.get("nome_articolo",""))
    if not name:
        print("[SKIP] Nome mancante.")
        return None

    sku = (row.get("sku","") or "").strip()
    if not sku:
        print(f"[SKIP] {name}: SKU mancante.")
        return None

    price = parse_price(row.get("prezzo_eur",""))
    brand = (row.get("brand") or "").strip()
    category = (row.get("categoria") or "").strip()
    deadline = (row.get("preorder_deadline") or "").strip()
    eta = (row.get("eta") or row.get("eta_trimestre") or "").strip()
    descr = (row.get("descrizione") or "").strip()

    # calcoli varianti
    price_as = round(price * 0.30, 2)
    price_pa = round(price * 0.95, 2)

    desc_html = build_description(deadline, eta, descr)

    # Oggetto prodotto per API v3
    product_obj = {
        "name": name,
        "sku": sku,
        "productType": "PHYSICAL",
        "priceData": {"price": price, "currency": "EUR"},
        "description": desc_html,
        "ribbon": {"text": "PREORDER"},
        "options": [
            {
                "name": OPT_TITLE,
                "selectionType": "SINGLE",
                "choices": [
                    {"value": CHOICE_AS, "description": "Acconto 30%"},
                    {"value": CHOICE_PA, "description": "Pagamento anticipato -5%"}
                ]
            }
        ]
    }
    if brand:
        product_obj["brand"] = {"name": brand}

    variants = [
        {
            "choices": {OPT_TITLE: CHOICE_AS},
            "priceData": {"price": price_as, "currency": "EUR"},
            "visible": True
        },
        {
            "choices": {OPT_TITLE: CHOICE_PA},
            "priceData": {"price": price_pa, "currency": "EUR"},
            "visible": True
        }
    ]

    existing = query_product_by_sku(sku)
    try:
        if existing:
            pid, rev = existing
            print(f"[UPD] {name} (SKU={sku}) id={pid}")
            product_obj["id"] = pid
            product_obj["revision"] = rev
            payload = {"product": product_obj, "variants": variants}
            r = req("PATCH", f"/stores/v3/products/{pid}", json_body=payload, ok=(200,))
            prod = r.json().get("product", {})
            prod_id = prod.get("id", pid)
        else:
            print(f"[NEW] {name} (SKU={sku})")
            payload = {"product": product_obj, "variants": variants}
            r = req("POST", "/stores/v3/products", json_body=payload, ok=(200,))
            prod = r.json().get("product", {})
            prod_id = prod.get("id")
            if not prod_id:
                die(f"{name}: ID prodotto non ricevuto.")
    except Exception as e:
        # Se il create/update v3 dovesse fallire per SKU duplicato o amenità strane, prova a leggere ancora e prosegui con categoria
        print(f"[WARN] v3 create/update fallita: {e}")
        existing = query_product_by_sku(sku)
        if not existing:
            raise
        prod_id, _ = existing

    # Categoria
    if category:
        add_to_category(prod_id, category)

    return prod_id

def main():
    if not API_KEY or not SITE_ID:
        die("WIX_API_KEY o WIX_SITE_ID mancanti nei secrets.")

    csv_path = sys.argv[1] if len(sys.argv) > 1 else "input/template_preordini_v7.csv"

    # Precheck
    try:
        # ping semplice: leggere qualche prodotto visibile
        r = req("GET", "/stores-reader/v1/products", params={"limit": 1})
        vis = len(r.json().get("products", []))
        print(f"[PRECHECK] API ok. Prodotti visibili: {vis}")
    except Exception as e:
        die(f"API non raggiungibili: {e}")

    load_categories()

    print(f"[INFO] CSV: {csv_path}")
    created = 0
    with open(csv_path, "r", encoding="utf-8-sig", newline="") as fh:
        rdr = csv.DictReader(fh, delimiter=";")
        required = {"nome_articolo","prezzo_eur","sku"}
        missing = [c for c in required if c not in rdr.fieldnames]
        if missing:
            die(f"CSV mancano colonne obbligatorie: {missing}")

        for i, row in enumerate(rdr, start=2):
            name = (row.get("nome_articolo") or "").strip()
            if not name or not (row.get("prezzo_eur") or "").strip():
                print(f"[SKIP] Riga {i}: nome_articolo o prezzo_eur mancante.")
                continue
            try:
                pid = create_or_update(row)
                if pid:
                    created += 1
                    print(f"[OK] Riga {i} -> id {pid}")
            except Exception as e:
                print(f"[ERRORE] Riga {i} '{name}': {e}")

    if created == 0:
        die("Nessun prodotto creato/aggiornato.")
    print(f"[FINE] Prodotti creati/aggiornati: {created}")

if __name__ == "__main__":
    main()
