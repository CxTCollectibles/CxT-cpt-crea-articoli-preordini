#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import csv
import json
import os
import sys
import time
import html
from typing import Dict, Any, Optional, Tuple, List

import requests

# ---------------------------
# Config base
# ---------------------------

CSV_REQUIRED_COLS = [
    "nome_articolo",
    "prezzo_eur",
    "sku",
    "brand",
    "categoria",
    "descrizione",
    "preorder_scadenza",
    "eta",
]

API_BASE = "https://www.wixapis.com"
PRODUCTS_QUERY = f"{API_BASE}/stores/v1/products/query"
PRODUCTS_CREATE = f"{API_BASE}/stores/v1/products"
PRODUCT_PATCH_TPL = f"{API_BASE}/stores/v1/products/{{product_id}}"
PRODUCT_VARIANTS_PATCH_TPL = f"{API_BASE}/stores/v1/products/{{product_id}}/variants"
COLLECTIONS_QUERY = f"{API_BASE}/stores/v1/collections/query"
COLLECTION_ADD_TPL = f"{API_BASE}/stores/v1/collections/{{collection_id}}/products/add"

# Nomi fissi concordati
OPTION_TITLE = "PREORDER PAYMENTS OPTIONS*"
CHOICE_DEPOSIT = "ANTICIPO/SALDO"
CHOICE_PREPAID = "PAGAMENTO ANTICIPATO"

CHOICE_DEPOSIT_DESC = "Paga 30% ora, saldo alla disponibilità"
CHOICE_PREPAID_DESC = "Pagamento immediato con sconto 5%"

# ---------------------------
# Utils
# ---------------------------

def env_or_die(name: str) -> str:
    val = os.getenv(name, "").strip()
    if not val:
        print(f"[FATAL] Variabile {name} mancante.")
        sys.exit(2)
    return val

def wix_headers() -> Dict[str, str]:
    api_key = env_or_die("WIX_API_KEY")
    site_id = env_or_die("WIX_SITE_ID")
    return {
        "Authorization": api_key,
        "wix-site-id": site_id,
        "Content-Type": "application/json",
    }

def http_post(url: str, payload: Dict[str, Any]) -> requests.Response:
    h = wix_headers()
    r = requests.post(url, headers=h, json=payload, timeout=30)
    return r

def http_patch(url: str, payload: Dict[str, Any]) -> requests.Response:
    h = wix_headers()
    r = requests.patch(url, headers=h, json=payload, timeout=30)
    return r

def to_float(x: str) -> float:
    x = x.replace("€", "").replace(",", ".").strip()
    return round(float(x), 2)

def sanitize_name(name: str) -> str:
    name = name.strip()
    if len(name) > 80:
        print("[WARN] Nome > 80 caratteri, troncato.")
        name = name[:80]
    return name

def build_description_html(deadline: str, eta: str, body_txt: str) -> str:
    # Escapa HTML e sostituisci newline con <br>
    header_txt = f"PREORDER DEADLINE: {deadline}\nETA: {eta}\n\n"
    full_txt = header_txt + (body_txt or "").strip()
    esc = html.escape(full_txt)
    esc = esc.replace("\r\n", "\n").replace("\r", "\n").replace("\n", "<br>")
    return f"<div><p>{esc}</p></div>"

def calc_prices(base: float) -> Tuple[float, float]:
    deposit = round(base * 0.30, 2)
    prepaid = round(base * 0.95, 2)
    # Evita 0 per siti con arrotondamenti strani
    if deposit <= 0:
        deposit = 0.01
    if prepaid <= 0:
        prepaid = 0.01
    return deposit, prepaid

def read_csv_rows(csv_path: str) -> List[Dict[str, str]]:
    with open(csv_path, "r", encoding="utf-8-sig", newline="") as fh:
        sniffer = csv.Sniffer()
        sample = fh.read(4096)
        fh.seek(0)
        dialect = sniffer.sniff(sample, delimiters=";,")
        reader = csv.DictReader(fh, dialect=dialect)
        cols = [c.strip() for c in reader.fieldnames or []]
        missing = [c for c in CSV_REQUIRED_COLS if c not in cols]
        if missing:
            raise RuntimeError(f"CSV mancano colonne: {missing}")
        rows = []
        for row in reader:
            rows.append({k.strip(): (v or "").strip() for k, v in row.items()})
        return rows

# ---------------------------
# Wix helpers
# ---------------------------

def query_product_by_sku(sku: str) -> Optional[Dict[str, Any]]:
    payload = {
        "query": {
            "filter": {"sku": {"$eq": sku}},
            "paging": {"limit": 1}
        }
    }
    r = http_post(PRODUCTS_QUERY, payload)
    if r.status_code == 200:
        data = r.json()
        items = (data or {}).get("products") or (data or {}).get("items") or []
        return items[0] if items else None
    # 404/400 etc
    print(f"[WARN] Query SKU fallita {sku}: {r.status_code} {r.text[:200]}")
    return None

def ensure_collection_id_by_name(name: str) -> Optional[str]:
    # Usa /collections/query perché i GET danno 404 nel tuo sito
    payload = {"query": {"filter": {}, "paging": {"limit": 100}}}
    r = http_post(COLLECTIONS_QUERY, payload)
    if r.status_code != 200:
        print(f"[WARN] Lettura categorie fallita: {r.status_code} {r.text[:200]}")
        return None
    data = r.json()
    items = data.get("collections") or data.get("items") or []
    name_l = name.strip().lower()
    for it in items:
        n = (it.get("name") or "").strip().lower()
        if n == name_l:
            return it.get("id") or it.get("_id")
    # non trovata
    print(f"[WARN] Collection '{name}' non trovata fra {len(items)} collezioni.")
    return None

def add_product_to_collection(product_id: str, collection_id: str) -> bool:
    url = COLLECTION_ADD_TPL.format(collection_id=collection_id)
    payload = {"productIds": [product_id]}
    r = http_post(url, payload)
    if r.status_code == 200:
        return True
    print(f"[WARN] Aggancio a collection fallito: {r.status_code} {r.text[:200]}")
    return False

def create_or_update_product(row: Dict[str, str]) -> Tuple[str, bool]:
    """
    Ritorna (product_id, created_bool)
    """
    name = sanitize_name(row["nome_articolo"])
    price = to_float(row["prezzo_eur"])
    sku = row["sku"].strip()
    brand = row["brand"].strip()
    categoria = row["categoria"].strip()
    descr_txt = row["descrizione"]
    deadline = row["preorder_scadenza"]
    eta = row["eta"]

    # Costruisci description HTML
    descr_html = build_description_html(deadline, eta, descr_txt)

    # Cerca già esistente per SKU
    existing = query_product_by_sku(sku)
    created = False

    if not existing:
        # CREATE
        payload = {
            "product": {
                "name": name,
                "productType": "physical",
                "sku": sku,
                "description": descr_html,
                "ribbon": "PREORDER",
                "priceData": {"price": price},
                "brand": {"name": brand} if brand else None,
                "manageVariants": True,
                "productOptions": [
                    {
                        "name": OPTION_TITLE,
                        "choices": [
                            {"value": CHOICE_DEPOSIT, "description": CHOICE_DEPOSIT_DESC},
                            {"value": CHOICE_PREPAID, "description": CHOICE_PREPAID_DESC},
                        ],
                    }
                ],
                # visibilità default: true
            }
        }
        # pulizia None
        payload["product"] = {k: v for k, v in payload["product"].items() if v is not None}

        r = http_post(PRODUCTS_CREATE, payload)
        if r.status_code != 200:
            raise RuntimeError(f"POST /products fallita: {r.status_code} {r.text}")
        data = r.json()
        product_id = (data.get("product") or {}).get("id") or (data.get("id"))
        if not product_id:
            raise RuntimeError("ID prodotto non ricevuto in create")
        created = True
    else:
        # UPDATE base fields + options
        product_id = existing.get("id")
        patch_url = PRODUCT_PATCH_TPL.format(product_id=product_id)
        patch_payload = {
            "product": {
                "name": name,
                "productType": "physical",
                "description": descr_html,
                "priceData": {"price": price},
                "brand": {"name": brand} if brand else None,
                "ribbon": "PREORDER",
                "manageVariants": True,
                "productOptions": [
                    {
                        "name": OPTION_TITLE,
                        "choices": [
                            {"value": CHOICE_DEPOSIT, "description": CHOICE_DEPOSIT_DESC},
                            {"value": CHOICE_PREPAID, "description": CHOICE_PREPAID_DESC},
                        ],
                    }
                ],
            }
        }
        # pulizia None
        patch_payload["product"] = {k: v for k, v in patch_payload["product"].items() if v is not None}
        r = http_patch(patch_url, patch_payload)
        if r.status_code != 200:
            raise RuntimeError(f"PATCH /products/{product_id} fallita: {r.status_code} {r.text}")

    # Varianti: DEVONO essere gestite e in forma di oggetto per 'choices'
    deposit_price, prepaid_price = calc_prices(price)
    variants_payload = {
        "variants": [
            {
                "choices": {OPTION_TITLE: CHOICE_DEPOSIT},
                "visible": True,
                "inStock": True,
                "priceData": {"price": deposit_price},
            },
            {
                "choices": {OPTION_TITLE: CHOICE_PREPAID},
                "visible": True,
                "inStock": True,
                "priceData": {"price": prepaid_price},
            },
        ]
    }
    v_url = PRODUCT_VARIANTS_PATCH_TPL.format(product_id=product_id)
    rv = http_patch(v_url, variants_payload)
    if rv.status_code != 200:
        # Se il sito richiede prima un "reset", riprova forzando manageVariants
        time.sleep(0.5)
        reset_payload = {
            "product": {
                "manageVariants": True,
                "productOptions": [
                    {
                        "name": OPTION_TITLE,
                        "choices": [
                            {"value": CHOICE_DEPOSIT, "description": CHOICE_DEPOSIT_DESC},
                            {"value": CHOICE_PREPAID, "description": CHOICE_PREPAID_DESC},
                        ],
                    }
                ],
            }
        }
        r2 = http_patch(PRODUCT_PATCH_TPL.format(product_id=product_id), reset_payload)
        if r2.status_code == 200:
            rv = http_patch(v_url, variants_payload)
    if rv.status_code != 200:
        raise RuntimeError(f"PATCH /products/{product_id}/variants fallita: {rv.status_code} {rv.text}")

    # Categoria (collection) opzionale
    if categoria:
        coll_id = ensure_collection_id_by_name(categoria)
        if coll_id:
            add_product_to_collection(product_id, coll_id)

    return product_id, created

# ---------------------------
# Main
# ---------------------------

def main():
    if len(sys.argv) < 2:
        print("Uso: python3 wix_preorder_ingestion.py <percorso_csv>")
        sys.exit(2)

    csv_path = sys.argv[1]
    if not os.path.isfile(csv_path):
        print(f"[FATAL] CSV non trovato: {csv_path}")
        sys.exit(2)

    # Preflight leggero: POST /products/query deve rispondere 200
    try:
        r = http_post(PRODUCTS_QUERY, {"query": {"paging": {"limit": 1}}})
        if r.status_code != 200:
            print(f"[WARN] Precheck query prodotti: {r.status_code} {r.text[:200]}")
    except Exception as e:
        print(f"[WARN] Precheck query prodotti exception: {e}")

    try:
        rows = read_csv_rows(csv_path)
    except Exception as e:
        print(f"[FATAL] Lettura CSV fallita: {e}")
        sys.exit(2)

    created = 0
    updated = 0
    errors = 0
    for idx, row in enumerate(rows, start=2):  # header è riga 1
        name_preview = sanitize_name(row.get("nome_articolo", "") or "")
        sku_preview = row.get("sku", "")
        try:
            pid, is_created = create_or_update_product(row)
            if is_created:
                print(f"[NEW] {name_preview} (SKU={sku_preview}) id={pid}")
                created += 1
            else:
                print(f"[UPD] {name_preview} (SKU={sku_preview}) id={pid}")
                updated += 1
        except Exception as e:
            errors += 1
            print(f"[ERRORE] Riga {idx} '{name_preview}': {e}")

    print(f"[DONE] Creati: {created}, Aggiornati: {updated}, Errori: {errors}")
    if errors:
        sys.exit(2)

if __name__ == "__main__":
    main()
