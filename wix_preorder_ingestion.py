#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import csv
import html
import json
import os
import sys
import time
from typing import Any, Dict, List, Optional, Tuple

import requests

API_BASE = "https://www.wixapis.com"

ENDPOINTS = {
    "products_query": f"{API_BASE}/stores/v1/products/query",
    "products_create": f"{API_BASE}/stores/v1/products",
    "product_patch": f"{API_BASE}/stores/v1/products/{{product_id}}",
    "variants_patch": f"{API_BASE}/stores/v1/products/{{product_id}}/variants",
    "collections_query": f"{API_BASE}/stores/v1/collections/query",
    "collection_add": f"{API_BASE}/stores/v1/collections/{{collection_id}}/products/add",
}

REQUIRED_COLS = [
    "nome_articolo",
    "prezzo_eur",
    "sku",
    "brand",
    "categoria",
    "descrizione",
    "preorder_scadenza",
    "eta",
]

OPTION_TITLE = "PREORDER PAYMENTS OPTIONS*"
CHOICE_DEPOSIT = "ANTICIPO/SALDO"
CHOICE_PREPAID = "PAGAMENTO ANTICIPATO"
CHOICE_DEPOSIT_DESC = "Paga 30% ora, saldo alla disponibilità"
CHOICE_PREPAID_DESC = "Pagamento immediato con sconto 5%"

def env_or_die(name: str) -> str:
    v = os.getenv(name, "").strip()
    if not v:
        print(f"[FATAL] Variabile {name} mancante.")
        sys.exit(2)
    return v

def headers() -> Dict[str, str]:
    return {
        "Authorization": env_or_die("WIX_API_KEY"),
        "wix-site-id": env_or_die("WIX_SITE_ID"),
        "Content-Type": "application/json",
    }

def to_float(s: str) -> float:
    s = (s or "").replace("€", "").replace(",", ".").strip()
    return round(float(s), 2)

def sanitize_name(n: str) -> str:
    n = (n or "").strip()
    if len(n) > 80:
        print("[WARN] Nome > 80 caratteri, troncato.")
        n = n[:80]
    return n

def build_descr(deadline: str, eta: str, body: str) -> str:
    # Riga 1: PREORDER DEADLINE, riga 2: ETA, riga vuota, poi il testo
    raw = f"PREORDER DEADLINE: {deadline}\nETA: {eta}\n\n{body or ''}".strip()
    esc = html.escape(raw).replace("\r\n", "\n").replace("\r", "\n").replace("\n", "<br>")
    return f"<div><p>{esc}</p></div>"

def calc_prices(base: float) -> Tuple[float, float]:
    deposit = round(base * 0.30, 2)
    prepaid = round(base * 0.95, 2)
    if deposit <= 0: deposit = 0.01
    if prepaid <= 0: prepaid = 0.01
    return deposit, prepaid

def read_csv(path: str) -> List[Dict[str, str]]:
    with open(path, "r", encoding="utf-8-sig", newline="") as fh:
        sniff = csv.Sniffer()
        sample = fh.read(4096)
        fh.seek(0)
        dialect = sniff.sniff(sample, delimiters=";,")
        r = csv.DictReader(fh, dialect=dialect)
        cols = [c.strip() for c in (r.fieldnames or [])]
        miss = [c for c in REQUIRED_COLS if c not in cols]
        if miss:
            raise RuntimeError(f"CSV mancano colonne: {miss}")
        rows = []
        for row in r:
            rows.append({(k or "").strip(): (v or "").strip() for k, v in row.items()})
        return rows

def post(url: str, payload: Dict[str, Any]) -> requests.Response:
    return requests.post(url, headers=headers(), json=payload, timeout=30)

def patch(url: str, payload: Dict[str, Any]) -> requests.Response:
    return requests.patch(url, headers=headers(), json=payload, timeout=30)

def query_by_sku(sku: str) -> Optional[Dict[str, Any]]:
    # Niente $eq: Wix qui vuole valore diretto
    payload = {"query": {"filter": {"sku": sku}, "paging": {"limit": 1}}}
    r = post(ENDPOINTS["products_query"], payload)
    if r.status_code != 200:
        print(f"[WARN] Query SKU fallita {sku}: {r.status_code} {r.text[:200]}")
        return None
    data = r.json() or {}
    items = data.get("products") or data.get("items") or []
    return items[0] if items else None

def ensure_collection_id(name: str) -> Optional[str]:
    if not name.strip():
        return None
    r = post(ENDPOINTS["collections_query"], {"query": {"filter": {}, "paging": {"limit": 100}}})
    if r.status_code != 200:
        print(f"[WARN] Lettura categorie fallita: {r.status_code} {r.text[:200]}")
        return None
    items = (r.json() or {}).get("collections") or (r.json() or {}).get("items") or []
    name_l = name.strip().lower()
    for it in items:
        n = (it.get("name") or "").strip().lower()
        if n == name_l:
            return it.get("id") or it.get("_id")
    print(f"[WARN] Collection '{name}' non trovata tra {len(items)}.")
    return None

def add_to_collection(product_id: str, collection_id: str) -> None:
    url = ENDPOINTS["collection_add"].format(collection_id=collection_id)
    r = post(url, {"productIds": [product_id]})
    if r.status_code != 200:
        print(f"[WARN] Aggancio categoria fallito: {r.status_code} {r.text[:200]}")

def upsert(row: Dict[str, str]) -> Tuple[str, bool]:
    name = sanitize_name(row["nome_articolo"])
    price = to_float(row["prezzo_eur"])
    sku = row["sku"]
    brand = row["brand"]
    categoria = row["categoria"]
    descr = row["descrizione"]
    deadline = row["preorder_scadenza"]
    eta = row["eta"]

    descr_html = build_descr(deadline, eta, descr)

    existed = query_by_sku(sku)
    created = False

    # payload base (brand come stringa, non oggetto)
    base_product = {
        "name": name,
        "productType": "physical",
        "sku": sku,
        "description": descr_html,
        "ribbon": "PREORDER",
        "priceData": {"price": price},
        "brand": brand if brand else None,
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
    base_product = {k: v for k, v in base_product.items() if v is not None}

    if not existed:
        r = post(ENDPOINTS["products_create"], {"product": base_product})
        if r.status_code != 200:
            raise RuntimeError(f"POST /products fallita: {r.status_code} {r.text}")
        data = r.json() or {}
        product = data.get("product") or data
        product_id = product.get("id")
        if not product_id:
            raise RuntimeError("ID prodotto non ricevuto")
        created = True
    else:
        product_id = existed.get("id")
        url = ENDPOINTS["product_patch"].format(product_id=product_id)
        r = patch(url, {"product": base_product})
        if r.status_code != 200:
            raise RuntimeError(f"PATCH /products/{product_id} fallita: {r.status_code} {r.text}")

    dep, pre = calc_prices(price)
    v_url = ENDPOINTS["variants_patch"].format(product_id=product_id)
    variants_payload = {
        "variants": [
            {
                "choices": {OPTION_TITLE: CHOICE_DEPOSIT},
                "visible": True,
                "inStock": True,
                "priceData": {"price": dep},
            },
            {
                "choices": {OPTION_TITLE: CHOICE_PREPAID},
                "visible": True,
                "inStock": True,
                "priceData": {"price": pre},
            },
        ]
    }
    rv = patch(v_url, variants_payload)
    if rv.status_code != 200:
        # Forza setup opzioni e ritenta una volta
        reset = {
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
        patch(ENDPOINTS["product_patch"].format(product_id=product_id), reset)
        time.sleep(0.4)
        rv = patch(v_url, variants_payload)
        if rv.status_code != 200:
            raise RuntimeError(f"PATCH /products/{product_id}/variants fallita: {rv.status_code} {rv.text}")

    if categoria:
        cid = ensure_collection_id(categoria)
        if cid:
            add_to_collection(product_id, cid)

    return product_id, created

def main():
    if len(sys.argv) < 2:
        print("Uso: python3 wix_preorder_ingestion.py <percorso_csv>")
        sys.exit(2)

    csv_path = sys.argv[1]
    if not os.path.isfile(csv_path):
        print(f"[FATAL] CSV non trovato: {csv_path}")
        sys.exit(2)

    try:
        rows = read_csv(csv_path)
    except Exception as e:
        print(f"[FATAL] Lettura CSV fallita: {e}")
        sys.exit(2)

    created = 0
    updated = 0
    errors = 0

    for i, row in enumerate(rows, start=2):
        name = sanitize_name(row.get("nome_articolo", ""))
        sku = row.get("sku", "")
        try:
            pid, is_created = upsert(row)
            if is_created:
                print(f"[NEW] {name} (SKU={sku}) id={pid}")
                created += 1
            else:
                print(f"[UPD] {name} (SKU={sku}) id={pid}")
                updated += 1
        except Exception as e:
            errors += 1
            print(f"[ERRORE] Riga {i} '{name}': {e}")

    print(f"[DONE] Creati: {created}, Aggiornati: {updated}, Errori: {errors}")
    if errors:
        sys.exit(2)

if __name__ == "__main__":
    main()
