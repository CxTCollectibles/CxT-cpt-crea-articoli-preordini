#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import csv
import html
import json
import os
import sys
from typing import Any, Dict, List, Optional

import requests

API = "https://www.wixapis.com"

# Endpoints usati (tutti Stores v1)
EP_PRODUCTS_CREATE = f"{API}/stores/v1/products"
EP_PRODUCT_PATCH   = f"{API}/stores/v1/products/{{pid}}"
EP_PRODUCTS_QUERY  = f"{API}/stores/v1/products/query"

EP_COLLECTIONS_QUERY = f"{API}/stores/v1/collections/query"
EP_COLLECTION_ADD    = f"{API}/stores/v1/collections/{{cid}}/products/add"

# Colonne attese dal tuo CSV V7
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

def need_env(name: str) -> str:
    v = os.getenv(name, "").strip()
    if not v:
        print(f"[FATAL] Variabile {name} mancante.")
        sys.exit(2)
    return v

def headers() -> Dict[str, str]:
    return {
        "Authorization": need_env("WIX_API_KEY"),
        "wix-site-id": need_env("WIX_SITE_ID"),
        "Content-Type": "application/json",
    }

def to_price(s: str) -> float:
    s = (s or "").replace("€", "").replace(",", ".").strip()
    try:
        return round(float(s), 2)
    except Exception:
        return 0.0

def short_name(name: str) -> str:
    name = (name or "").strip()
    if len(name) > 80:
        print("[WARN] Nome > 80 caratteri, troncato.")
        name = name[:80]
    return name

def build_description(deadline: str, eta: str, body: str) -> str:
    # Riga vuota tra ETA e corpo
    raw = f"PREORDER DEADLINE: {deadline}\nETA: {eta}\n\n{body or ''}".strip()
    esc = html.escape(raw).replace("\r\n", "\n").replace("\r", "\n").replace("\n", "<br>")
    return f"<div><p>{esc}</p></div>"

def read_csv(path: str) -> List[Dict[str, str]]:
    if not os.path.isfile(path):
        print(f"[FATAL] CSV non trovato: {path}")
        sys.exit(2)
    with open(path, "r", encoding="utf-8-sig", newline="") as fh:
        sample = fh.read(4096)
        fh.seek(0)
        # Sniff delimitatore ; o ,
        try:
            dialect = csv.Sniffer().sniff(sample, delimiters=";,")
        except Exception:
            class D(csv.Dialect):
                delimiter = ';'
                quotechar = '"'
                doublequote = True
                skipinitialspace = False
                lineterminator = '\n'
                quoting = csv.QUOTE_MINIMAL
            dialect = D
        rdr = csv.DictReader(fh, dialect=dialect)
        cols = [c.strip() for c in (rdr.fieldnames or [])]
        missing = [c for c in REQUIRED_COLS if c not in cols]
        if missing:
            raise RuntimeError(f"CSV mancano colonne: {missing}")
        rows: List[Dict[str, str]] = []
        for row in rdr:
            rows.append({(k or "").strip(): (v or "").strip() for k, v in row.items()})
        return rows

def post(url: str, payload: Dict[str, Any]) -> requests.Response:
    return requests.post(url, headers=headers(), json=payload, timeout=30)

def patch(url: str, payload: Dict[str, Any]) -> requests.Response:
    return requests.patch(url, headers=headers(), json=payload, timeout=30)

def query_product_by_sku(sku: str) -> Optional[Dict[str, Any]]:
    """Cerca il prodotto per SKU con l'endpoint ufficiale di query."""
    payload = {
        "query": {
            "filter": {
                "sku": str(sku)  # forziamo stringa
            },
            "paging": {"limit": 1}
        }
    }
    r = post(EP_PRODUCTS_QUERY, payload)
    if r.status_code != 200:
        print(f"[WARN] Query SKU fallita {sku}: {r.status_code} {r.text[:160]}")
        return None
    data = r.json() or {}
    items = data.get("products") or data.get("items") or []
    return items[0] if items else None

def find_collection_id(name: str) -> Optional[str]:
    nm = (name or "").strip()
    if not nm:
        return None
    r = post(EP_COLLECTIONS_QUERY, {"query": {"filter": {}, "paging": {"limit": 100}}})
    if r.status_code != 200:
        print(f"[WARN] Lettura categorie fallita: {r.status_code} {r.text[:160]}")
        return None
    data = r.json() or {}
    items = data.get("collections") or data.get("items") or []
    target = nm.lower()
    for it in items:
        if (it.get("name") or "").strip().lower() == target:
            return it.get("id") or it.get("_id")
    print(f"[WARN] Collection '{nm}' non trovata.")
    return None

def add_to_collection(pid: str, cid: str) -> None:
    r = post(EP_COLLECTION_ADD.format(cid=cid), {"productIds": [pid]})
    if r.status_code != 200:
        print(f"[WARN] Aggancio categoria fallito: {r.status_code} {r.text[:160]}")

def make_product_payload(row: Dict[str, str]) -> Dict[str, Any]:
    name = short_name(row.get("nome_articolo", ""))
    price = to_price(row.get("prezzo_eur", ""))
    sku = (row.get("sku") or "").strip()
    brand = (row.get("brand") or "").strip()
    deadline = row.get("preorder_scadenza") or ""
    eta = row.get("eta") or ""
    body = row.get("descrizione") or ""
    descr_html = build_description(deadline, eta, body)

    payload: Dict[str, Any] = {
        "product": {
            "name": name,
            "productType": "physical",       # enum accettato: physical | digital
            "sku": sku,
            "description": descr_html,
            "ribbon": "PREORDER",
            "priceData": {"price": price},
        }
    }
    if brand:
        payload["product"]["brand"] = brand
    return payload

def upsert_row(row: Dict[str, str]) -> str:
    name = short_name(row.get("nome_articolo", ""))
    sku  = (row.get("sku") or "").strip()
    cat  = (row.get("categoria") or "").strip()

    # Proviamo prima a creare
    payload = make_product_payload(row)
    r = post(EP_PRODUCTS_CREATE, payload)
    if r.status_code == 200:
        data = r.json() or {}
        prod = data.get("product") or data
        pid = prod.get("id")
        if not pid:
            raise RuntimeError("ID prodotto non ricevuto.")
        print(f"[NEW] {name} (SKU={sku})")
    else:
        # Se SKU duplicato, cerchiamo il prodotto e facciamo PATCH
        txt = (r.text or "").lower()
        if r.status_code == 400 and "sku is not unique" in txt:
            existing = query_product_by_sku(sku)
            if not existing:
                raise RuntimeError(f"SKU duplicato ma prodotto non trovato: {sku}")
            pid = existing.get("id")
            r2 = patch(EP_PRODUCT_PATCH.format(pid=pid), payload)
            if r2.status_code != 200:
                raise RuntimeError(f"PATCH fallita: {r2.status_code} {r2.text}")
            print(f"[UPD] {name} (SKU={sku})")
        else:
            raise RuntimeError(f"POST fallita: {r.status_code} {r.text}")

    # Categoria (best-effort)
    if cat:
        cid = find_collection_id(cat)
        if cid:
            add_to_collection(pid, cid)

    return pid

def main():
    if len(sys.argv) < 2:
        print("Uso: python3 wix_preorder_ingestion.py <CSV_V7>")
        sys.exit(2)
    csv_path = sys.argv[1]
    rows = read_csv(csv_path)

    errors = 0
    for i, row in enumerate(rows, start=2):
        name = short_name(row.get("nome_articolo", ""))
        try:
            upsert_row(row)
        except Exception as e:
            errors += 1
            print(f"[ERRORE] Riga {i} '{name}': {e}")

    print(f"[DONE] Errori: {errors}")
    if errors:
        sys.exit(2)

if __name__ == "__main__":
    main()
