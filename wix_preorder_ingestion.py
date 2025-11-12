#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import csv
import html
import os
import sys
from typing import Any, Dict, List, Optional, Tuple

import requests

API = "https://www.wixapis.com"
EP = {
    "products_query": f"{API}/stores/v1/products/query",
    "products": f"{API}/stores/v1/products",
    "product_patch": f"{API}/stores/v1/products/{{pid}}",
    "variants_patch": f"{API}/stores/v1/products/{{pid}}/variants",
    "collections_query": f"{API}/stores/v1/collections/query",
    "collection_add": f"{API}/stores/v1/collections/{{cid}}/products/add",
}

# CSV atteso
REQ_COLS = [
    "nome_articolo",
    "prezzo_eur",
    "sku",
    "brand",
    "categoria",
    "descrizione",
    "preorder_scadenza",
    "eta",
]

# Opzioni e scelte richieste
OPT_TITLE = "PREORDER PAYMENTS OPTIONS*"
CH_DEPOSIT = "ANTICIPO/SALDO"
CH_PREPAID = "PAGAMENTO ANTICIPATO"
DESC_DEPOSIT = "Paga 30% ora, saldo alla disponibilità"
DESC_PREPAID = "Pagamento immediato con sconto 5%"

def need_env(name: str) -> str:
    v = os.getenv(name, "").strip()
    if not v:
        print(f"[FATAL] Variabile {name} mancante.")
        sys.exit(2)
    return v

def hdrs() -> Dict[str, str]:
    return {
        "Authorization": need_env("WIX_API_KEY"),
        "wix-site-id": need_env("WIX_SITE_ID"),
        "Content-Type": "application/json",
    }

def to_float(v: str) -> float:
    v = (v or "").replace("€", "").replace(",", ".").strip()
    return round(float(v), 2)

def short_name(name: str) -> str:
    name = (name or "").strip()
    if len(name) > 80:
        print("[WARN] Nome > 80 caratteri, troncato.")
        name = name[:80]
    return name

def make_descr(deadline: str, eta: str, body: str) -> str:
    # Riga vuota tra ETA e corpo
    raw = f"PREORDER DEADLINE: {deadline}\nETA: {eta}\n\n{body or ''}".strip()
    esc = html.escape(raw).replace("\r\n", "\n").replace("\r", "\n").replace("\n", "<br>")
    return f"<div><p>{esc}</p></div>"

def read_csv(path: str) -> List[Dict[str, str]]:
    with open(path, "r", encoding="utf-8-sig", newline="") as fh:
        sample = fh.read(4096)
        fh.seek(0)
        dialect = csv.Sniffer().sniff(sample, delimiters=";,")
        r = csv.DictReader(fh, dialect=dialect)
        cols = [c.strip() for c in (r.fieldnames or [])]
        missing = [c for c in REQ_COLS if c not in cols]
        if missing:
            raise RuntimeError(f"CSV mancano colonne: {missing}")
        rows: List[Dict[str, str]] = []
        for row in r:
            rows.append({(k or "").strip(): (v or "").strip() for k, v in row.items()})
        return rows

def rq_post(url: str, payload: Dict[str, Any]) -> requests.Response:
    return requests.post(url, headers=hdrs(), json=payload, timeout=30)

def rq_patch(url: str, payload: Dict[str, Any]) -> requests.Response:
    return requests.patch(url, headers=hdrs(), json=payload, timeout=30)

def query_sku(sku: str) -> Optional[Dict[str, Any]]:
    # Tentativo A: filtro canonico
    p1 = {"query": {"filter": {"sku": {"$eq": sku}}, "paging": {"limit": 1}}}
    r1 = rq_post(EP["products_query"], p1)
    if r1.status_code == 200:
        data = r1.json() or {}
        items = data.get("products") or data.get("items") or []
        return items[0] if items else None
    # Tentativo B: valore diretto (evita "Unexpected value for StringValue")
    print(f"[WARN] Query SKU fallita {sku}: {r1.status_code} {r1.text[:160]}")
    p2 = {"query": {"filter": {"sku": sku}, "paging": {"limit": 1}}}
    r2 = rq_post(EP["products_query"], p2)
    if r2.status_code == 200:
        data = r2.json() or {}
        items = data.get("products") or data.get("items") or []
        return items[0] if items else None
    print(f"[WARN] Query SKU (fallback) fallita {sku}: {r2.status_code} {r2.text[:160]}")
    return None

def find_collection_id(name: str) -> Optional[str]:
    if not name.strip():
        return None
    # Query paginata semplice
    r = rq_post(EP["collections_query"], {"query": {"filter": {}, "paging": {"limit": 100}}})
    if r.status_code != 200:
        print(f"[WARN] Lettura categorie fallita: {r.status_code} {r.text[:160]}")
        return None
    data = r.json() or {}
    items = data.get("collections") or data.get("items") or []
    target = name.strip().lower()
    for it in items:
        nm = (it.get("name") or "").strip().lower()
        if nm == target:
            return it.get("id") or it.get("_id")
    print(f"[WARN] Collection '{name}' non trovata tra {len(items)}.")
    return None

def add_to_collection(pid: str, cid: str) -> None:
    r = rq_post(EP["collection_add"].format(cid=cid), {"productIds": [pid]})
    if r.status_code != 200:
        print(f"[WARN] Aggancio categoria fallito: {r.status_code} {r.text[:160]}")

def calc_prices(base: float) -> Tuple[float, float]:
    deposit = round(base * 0.30, 2)
    prepaid = round(base * 0.95, 2)
    if deposit <= 0: deposit = 0.01
    if prepaid <= 0: prepaid = 0.01
    return deposit, prepaid

def ensure_options(pid: str) -> None:
    # Definiamo le opzioni con value e description uguali per massima compatibilità
    payload = {
        "product": {
            "manageVariants": True,
            "productOptions": [
                {
                    "name": OPT_TITLE,
                    "choices": [
                        {"value": CH_DEPOSIT, "description": DESC_DEPOSIT},
                        {"value": CH_PREPAID, "description": DESC_PREPAID},
                    ],
                }
            ],
        }
    }
    r = rq_patch(EP["product_patch"].format(pid=pid), payload)
    if r.status_code != 200:
        raise RuntimeError(f"PATCH options fallita: {r.status_code} {r.text}")

def patch_variants(pid: str, dep: float, pre: float) -> None:
    url = EP["variants_patch"].format(pid=pid)

    # A) choices = [{"title": OPT_TITLE, "value": <choice>}]
    pA = {
        "variants": [
            {"choices": [{"title": OPT_TITLE, "value": CH_DEPOSIT}], "visible": True, "inStock": True, "priceData": {"price": dep}},
            {"choices": [{"title": OPT_TITLE, "value": CH_PREPAID}], "visible": True, "inStock": True, "priceData": {"price": pre}},
        ]
    }
    rA = rq_patch(url, pA)
    if rA.status_code == 200:
        return

    # B) choices = [{"title": OPT_TITLE, "description": <choice>}]
    pB = {
        "variants": [
            {"choices": [{"title": OPT_TITLE, "description": CH_DEPOSIT}], "visible": True, "inStock": True, "priceData": {"price": dep}},
            {"choices": [{"title": OPT_TITLE, "description": CH_PREPAID}], "visible": True, "inStock": True, "priceData": {"price": pre}},
        ]
    }
    rB = rq_patch(url, pB)
    if rB.status_code == 200:
        return

    # C) choices = { OPT_TITLE: <choice> }
    pC = {
        "variants": [
            {"choices": {OPT_TITLE: CH_DEPOSIT}, "visible": True, "inStock": True, "priceData": {"price": dep}},
            {"choices": {OPT_TITLE: CH_PREPAID}, "visible": True, "inStock": True, "priceData": {"price": pre}},
        ]
    }
    rC = rq_patch(url, pC)
    if rC.status_code == 200:
        return

    raise RuntimeError(
        f"PATCH variants fallita: A[{rA.status_code}] {rA.text[:120]} | B[{rB.status_code}] {rB.text[:120]} | C[{rC.status_code}] {rC.text[:120]}"
    )

def upsert(row: Dict[str, str]) -> Tuple[str, bool]:
    name = short_name(row["nome_articolo"])
    price = to_float(row["prezzo_eur"])
    sku = row["sku"]
    brand = row["brand"]
    cat = row["categoria"]
    descr = row["descrizione"]
    deadline = row["preorder_scadenza"]
    eta = row["eta"]

    descr_html = make_descr(deadline, eta, descr)

    product_payload = {
        "name": name,
        "productType": 1,  # physical
        "sku": sku,
        "description": descr_html,
        "ribbon": "PREORDER",
        "priceData": {"price": price},
        "brand": brand if brand else None,
        "manageVariants": True,
        "productOptions": [
            {
                "name": OPT_TITLE,
                "choices": [
                    {"value": CH_DEPOSIT, "description": DESC_DEPOSIT},
                    {"value": CH_PREPAID, "description": DESC_PREPAID},
                ],
            }
        ],
    }
    # ripulisci None
    product_payload = {k: v for k, v in product_payload.items() if v is not None}

    existing = query_sku(sku)
    created = False
    if existing is None:
        r = rq_post(EP["products"], {"product": product_payload})
        if r.status_code != 200:
            raise RuntimeError(f"POST /products fallita: {r.status_code} {r.text}")
        prod = (r.json() or {}).get("product") or r.json() or {}
        pid = prod.get("id")
        if not pid:
            raise RuntimeError("ID prodotto non ricevuto.")
        created = True
    else:
        pid = existing.get("id")
        r = rq_patch(EP["product_patch"].format(pid=pid), {"product": product_payload})
        if r.status_code != 200:
            raise RuntimeError(f"PATCH /products/{pid} fallita: {r.status_code} {r.text}")

    # Assicura opzioni e varianti
    ensure_options(pid)
    dep, pre = calc_prices(price)
    patch_variants(pid, dep, pre)

    # Categoria
    if cat:
        cid = find_collection_id(cat)
        if cid:
            add_to_collection(pid, cid)

    return pid, created

def main():
    if len(sys.argv) < 2:
        print("Uso: python3 wix_preorder_ingestion.py <csv>")
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

    created = updated = errors = 0
    for i, row in enumerate(rows, start=2):
        name = short_name(row.get("nome_articolo", ""))
        sku = row.get("sku", "")
        try:
            pid, was_created = upsert(row)
            if was_created:
                print(f"[NEW] {name} (SKU={sku})")
                created += 1
            else:
                print(f"[UPD] {name} (SKU={sku})")
                updated += 1
        except Exception as e:
            errors += 1
            print(f"[ERRORE] Riga {i} '{name}': {e}")

    print(f"[DONE] Creati: {created}, Aggiornati: {updated}, Errori: {errors}")
    if errors:
        sys.exit(2)

if __name__ == "__main__":
    main()
