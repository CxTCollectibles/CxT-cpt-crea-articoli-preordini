#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import csv
import html
import json
import os
import sys
from typing import Any, Dict, List, Optional, Tuple

import requests

API_BASE = "https://www.wixapis.com"
EP = {
    "prod_query": f"{API_BASE}/stores/v1/products/query",
    "prod_create": f"{API_BASE}/stores/v1/products",
    "prod_patch": f"{API_BASE}/stores/v1/products/{{pid}}",
    "variants_patch": f"{API_BASE}/stores/v1/products/{{pid}}/variants",
    "coll_query": f"{API_BASE}/stores/v1/collections/query",
    "coll_add": f"{API_BASE}/stores/v1/collections/{{cid}}/products/add",
}

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

def to_float(s: str) -> float:
    s = (s or "").replace("€", "").replace(",", ".").strip()
    return round(float(s), 2)

def short_name(n: str) -> str:
    n = (n or "").strip()
    if len(n) > 80:
        print("[WARN] Nome > 80 caratteri, troncato.")
        n = n[:80]
    return n

def make_descr(deadline: str, eta: str, body: str) -> str:
    raw = f"PREORDER DEADLINE: {deadline}\nETA: {eta}\n\n{body or ''}".strip()
    esc = html.escape(raw).replace("\r\n", "\n").replace("\r", "\n").replace("\n", "<br>")
    return f"<div><p>{esc}</p></div>"

def calc_prices(base: float) -> Tuple[float, float]:
    dep = round(base * 0.30, 2)
    pre = round(base * 0.95, 2)
    if dep <= 0: dep = 0.01
    if pre <= 0: pre = 0.01
    return dep, pre

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
        out = []
        for row in r:
            out.append({(k or "").strip(): (v or "").strip() for k, v in row.items()})
        return out

def rq_post(url: str, payload: Dict[str, Any]) -> requests.Response:
    return requests.post(url, headers=hdrs(), json=payload, timeout=30)

def rq_patch(url: str, payload: Dict[str, Any]) -> requests.Response:
    return requests.patch(url, headers=hdrs(), json=payload, timeout=30)

def query_sku(sku: str) -> Optional[Dict[str, Any]]:
    # Tentativo A: $eq
    p1 = {"query": {"filter": {"sku": {"$eq": sku}}, "paging": {"limit": 1}}}
    r = rq_post(EP["prod_query"], p1)
    if r.status_code == 200:
        data = r.json() or {}
        items = data.get("products") or data.get("items") or []
        return items[0] if items else None
    # Tentativo B: valore diretto
    print(f"[WARN] Query SKU fallita {sku}: {r.status_code} {r.text[:200]}")
    p2 = {"query": {"filter": {"sku": sku}, "paging": {"limit": 1}}}
    r2 = rq_post(EP["prod_query"], p2)
    if r2.status_code == 200:
        data = r2.json() or {}
        items = data.get("products") or data.get("items") or []
        return items[0] if items else None
    print(f"[WARN] Query SKU (fallback) fallita {sku}: {r2.status_code} {r2.text[:200]}")
    return None

def find_collection_id(name: str) -> Optional[str]:
    if not name.strip():
        return None
    r = rq_post(EP["coll_query"], {"query": {"filter": {}, "paging": {"limit": 100}}})
    if r.status_code != 200:
        print(f"[WARN] Lettura categorie fallita: {r.status_code} {r.text[:200]}")
        return None
    data = r.json() or {}
    items = data.get("collections") or data.get("items") or []
    name_l = name.strip().lower()
    for it in items:
        if (it.get("name") or "").strip().lower() == name_l:
            return it.get("id") or it.get("_id")
    print(f"[WARN] Collection '{name}' non trovata tra {len(items)}.")
    return None

def add_to_collection(pid: str, cid: str) -> None:
    r = rq_post(EP["coll_add"].format(cid=cid), {"productIds": [pid]})
    if r.status_code != 200:
        print(f"[WARN] Aggancio categoria fallito: {r.status_code} {r.text[:200]}")

def ensure_options(pid: str) -> None:
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
    r = rq_patch(EP["prod_patch"].format(pid=pid), payload)
    if r.status_code != 200:
        raise RuntimeError(f"PATCH options fallita: {r.status_code} {r.text}")

def set_variants_with_fallback(pid: str, dep: float, pre: float) -> None:
    url = EP["variants_patch"].format(pid=pid)

    # A) choices come lista di oggetti title/value
    pA = {
        "variants": [
            {
                "choices": [{"title": OPT_TITLE, "value": CH_DEPOSIT}],
                "visible": True,
                "inStock": True,
                "priceData": {"price": dep},
            },
            {
                "choices": [{"title": OPT_TITLE, "value": CH_PREPAID}],
                "visible": True,
                "inStock": True,
                "priceData": {"price": pre},
            },
        ]
    }
    r = rq_patch(url, pA)
    if r.status_code == 200:
        return

    # B) choices come title/description
    pB = {
        "variants": [
            {
                "choices": [{"title": OPT_TITLE, "description": CH_DEPOSIT}],
                "visible": True,
                "inStock": True,
                "priceData": {"price": dep},
            },
            {
                "choices": [{"title": OPT_TITLE, "description": CH_PREPAID}],
                "visible": True,
                "inStock": True,
                "priceData": {"price": pre},
            },
        ]
    }
    rB = rq_patch(url, pB)
    if rB.status_code == 200:
        return

    # C) choices come mappa { "Titolo": "Valore" }
    pC = {
        "variants": [
            {
                "choices": {OPT_TITLE: CH_DEPOSIT},
                "visible": True,
                "inStock": True,
                "priceData": {"price": dep},
            },
            {
                "choices": {OPT_TITLE: CH_PREPAID},
                "visible": True,
                "inStock": True,
                "priceData": {"price": pre},
            },
        ]
    }
    rC = rq_patch(url, pC)
    if rC.status_code == 200:
        return

    raise RuntimeError(
        f"PATCH variants fallita: A[{r.status_code}] {r.text[:120]} | "
        f"B[{rB.status_code}] {rB.text[:120]} | C[{rC.status_code}] {rC.text[:120]}"
    )

def upsert(row: Dict[str, str]) -> Tuple[str, bool]:
    name = short_name(row["nome_articolo"])
    price = to_float(row["prezzo_eur"])
    sku = row["sku"]
    brand = row["brand"]
    categoria = row["categoria"]
    descr = row["descrizione"]
    deadline = row["preorder_scadenza"]
    eta = row["eta"]

    descr_html = make_descr(deadline, eta, descr)

    base = {
        "name": name,
        "productType": 1,  # physical
        "sku": sku,
        "description": descr_html,
        "ribbon": "PREORDER",
        "priceData": {"price": price},
        "brand": brand if brand else None,  # stringa brand
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
    base = {k: v for k, v in base.items() if v is not None}

    existing = query_sku(sku)
    created = False
    if not existing:
        r = rq_post(EP["prod_create"], {"product": base})
        if r.status_code != 200:
            raise RuntimeError(f"POST /products fallita: {r.status_code} {r.text}")
        prod = (r.json() or {}).get("product") or r.json() or {}
        pid = prod.get("id")
        if not pid:
            raise RuntimeError("ID prodotto non ricevuto (create).")
        created = True
    else:
        pid = existing.get("id")
        r = rq_patch(EP["prod_patch"].format(pid=pid), {"product": base})
        if r.status_code != 200:
            raise RuntimeError(f"PATCH /products/{pid} fallita: {r.status_code} {r.text}")

    ensure_options(pid)
    dep, pre = calc_prices(price)
    set_variants_with_fallback(pid, dep, pre)

    if categoria:
        cid = find_collection_id(categoria)
        if cid:
            add_to_collection(pid, cid)

    return pid, created

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

    created = updated = errors = 0
    for i, row in enumerate(rows, start=2):
        name = short_name(row.get("nome_articolo", ""))
        sku = row.get("sku", "")
        try:
            pid, was_created = upsert(row)
            if was_created:
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
