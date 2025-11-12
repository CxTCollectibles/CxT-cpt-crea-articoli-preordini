#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import csv
import html
import json
import os
import re
import sys
import time
from itertools import islice
from typing import Dict, Any, Optional

import requests
from slugify import slugify
from unidecode import unidecode

API_BASE = "https://www.wixapis.com"

WIX_API_KEY = os.getenv("WIX_API_KEY", "").strip()
WIX_SITE_ID = os.getenv("WIX_SITE_ID", "").strip()

HEADERS = {
    "Content-Type": "application/json",
    "Authorization": WIX_API_KEY,
    "wix-site-id": WIX_SITE_ID,
}

# ---------- Utils

def log(msg):
    print(msg, flush=True)

def req(method: str, path: str, body: Optional[dict] = None, ok=(200,), headers=None):
    url = f"{API_BASE}{path}"
    h = dict(HEADERS)
    if headers:
        h.update(headers)
    data = json.dumps(body) if body is not None else None
    r = requests.request(method, url, headers=h, data=data, timeout=30)
    if r.status_code not in ok:
        raise requests.HTTPError(f"{method} {path} failed {r.status_code}: {r.text}")
    if r.text.strip():
        try:
            return r.json()
        except Exception:
            return {}
    return {}

def sanitize_name(n: str) -> str:
    n = (n or "").strip()
    if len(n) > 80:
        log("[WARN] Nome > 80 caratteri, troncato.")
        n = n[:80]
    return n

def mk_slug(name: str, sku: str) -> str:
    base = slugify(name) if name else ""
    s = (sku or "").lower().strip()
    if base and s:
        return f"{base}-{s}"
    return base or s or str(int(time.time()))

def fmt_money(v: float) -> float:
    return round(float(v), 2)

def eta_to_quarters(eta_text: str) -> str:
    # Intenzione: se è un mese (es. "agosto 2026") mappa a Q3 ecc.
    # fallback: usa il testo così com'è
    if not eta_text:
        return ""
    mesi_q = {
        1:"Q1",2:"Q1",3:"Q1",
        4:"Q2",5:"Q2",6:"Q2",
        7:"Q3",8:"Q3",9:"Q3",
        10:"Q4",11:"Q4",12:"Q4"
    }
    m_map = {
        'gen':'1','feb':'2','mar':'3','apr':'4','mag':'5','giu':'6',
        'lug':'7','ago':'8','set':'9','ott':'10','nov':'11','dic':'12'
    }
    t = eta_text.lower()
    for k,v in m_map.items():
        if k in t:
            try:
                m = int(v)
                q = mesi_q[m]
                # formato richiesto: "Qx - Qy"
                nxt = {"Q1":"Q2","Q2":"Q3","Q3":"Q4","Q4":"Q1"}.get(q,"Q4")
                return f"{q} - {nxt}"
            except Exception:
                break
    return eta_text

def build_description(deadline: str, eta: str, descr: str) -> str:
    parts = []
    if deadline:
        parts.append(f"<p><strong>PREORDER DEADLINE:</strong> {html.escape(deadline)}</p>")
    if eta:
        parts.append(f"<p><strong>ETA:</strong> {html.escape(eta_to_quarters(eta))}</p>")
    if parts:
        # riga vuota
        parts.append("<p>&nbsp;</p>")
    if descr:
        # preserva righe
        safe = html.escape(descr)
        safe = safe.replace("\r\n", "\n").replace("\r", "\n").replace("\n", "<br>")
        parts.append(f"<p>{safe}</p>")
    return "<div>" + "".join(parts) + "</div>"

def read_csv(path: str):
    with open(path, "r", encoding="utf-8-sig", newline="") as fh:
        sniffer = csv.Sniffer()
        sample = fh.read(4096)
        fh.seek(0)
        dialect = csv.excel
        delimiter = ";"
        if sniffer.has_header(sample):
            pass
        rdr = csv.DictReader(fh, delimiter=delimiter)
        required = ["nome_articolo","prezzo_eur","sku","brand","categoria","descrizione","preorder_deadline","eta"]
        missing = [c for c in required if c not in (rdr.fieldnames or [])]
        if missing:
            raise RuntimeError(f"CSV mancano colonne: {missing}")
        for i, row in enumerate(rdr, start=2):
            yield i, row

# ---------- Catalog helpers

def get_collections_map() -> Dict[str, str]:
    # REST: POST /stores/v1/collections/query oppure GET con limit
    # La doc consiglia query; alcune istanze rispondono al GET.
    # Provo GET, fallback a query.
    try:
        res = req("GET", "/stores/v1/collections?limit=100", None, ok=(200,))
        items = res.get("collections", []) or res.get("items", []) or []
    except Exception:
        # fallback query
        body = {"query": {"paging": {"limit": 100}}}
        res = req("POST", "/stores/v1/collections/query", body, ok=(200,))
        items = res.get("collections", []) or res.get("items", []) or []
    mp = {}
    for c in items:
        name = (c.get("name") or "").strip().lower()
        if name and c.get("id"):
            mp[name] = c["id"]
    return mp

def find_product_by_sku(sku: str) -> Optional[str]:
    # prova forma 1
    try:
        body = {"query": {"filter": {"sku": sku}, "paging": {"limit": 1}}}
        res = req("POST", "/stores/v1/products/query", body, ok=(200,))
        items = res.get("products", []) or res.get("items", [])
        if items:
            return items[0]["id"]
    except Exception:
        # forma 2 con $eq
        try:
            body = {"query": {"filter": {"sku": {"$eq": sku}}, "paging": {"limit": 1}}}
            res = req("POST", "/stores/v1/products/query", body, ok=(200,))
            items = res.get("products", []) or res.get("items", [])
            if items:
                return items[0]["id"]
        except Exception:
            return None
    return None

def ensure_brand_in_payload(brand_name: str) -> Dict[str, Any]:
    brand_name = (brand_name or "").strip()
    return {"name": brand_name} if brand_name else {}

def create_or_update_product(rownum: int, row: Dict[str, str], collections_map: Dict[str,str]):
    name_raw = (row.get("nome_articolo") or "").strip()
    price = fmt_money(row.get("prezzo_eur") or 0)
    if not name_raw or not price:
        log(f"[SKIP] Riga {rownum}: nome_articolo o prezzo_eur mancante.")
        return

    sku = (row.get("sku") or "").strip()
    brand = (row.get("brand") or "").strip()
    categoria = (row.get("categoria") or "").strip()
    descr = (row.get("descrizione") or "").strip()
    deadline = (row.get("preorder_deadline") or "").strip()
    eta = (row.get("eta") or "").strip()

    name = sanitize_name(name_raw)
    slug = mk_slug(name, sku)
    description_html = build_description(deadline, eta, descr)

    product_payload = {
        "product": {
            "name": name,
            "slug": slug,
            "productType": "physical",  # ATTENZIONE: minuscolo (doc ufficiale)
            "priceData": {"price": price},
            "sku": sku,
            "description": description_html,
            "ribbon": "PREORDER",
            "brand": ensure_brand_in_payload(brand),
            "manageVariants": True,
            "productOptions": [
                {
                    "name": "PREORDER PAYMENTS OPTIONS*",
                    "choices": [
                        {
                            "value": "ANTICIPO/SALDO",
                            "description": f"Acconto 30%: €{fmt_money(price*0.30)} | Saldo alla consegna"
                        },
                        {
                            "value": "PAGAMENTO ANTICIPATO",
                            "description": f"Sconto 5%: €{fmt_money(price*0.95)}"
                        }
                    ]
                }
            ],
            "visible": True
        }
    }

    existing_id = find_product_by_sku(sku) if sku else None

    if existing_id:
        # Update
        log(f"[UPD] Riga {rownum}: aggiorno '{name}' (SKU={sku})")
        try:
            req("PATCH", f"/stores/v1/products/{existing_id}", product_payload, ok=(200,))
        except Exception as e:
            raise RuntimeError(f"PATCH /products/{existing_id} fallita: {e}")
        product_id = existing_id
    else:
        # Create
        log(f"[NEW] {name} (SKU={sku})")
        try:
            res = req("POST", "/stores/v1/products", product_payload, ok=(200,))
        except Exception as e:
            raise RuntimeError(f"POST /products fallita: {e}")
        product_id = (res.get("product") or {}).get("id") or res.get("id")
        if not product_id:
            raise RuntimeError("ID prodotto non ricevuto.")

    # Aggiorna varianti prezzi
    try:
        variant_body = {
            "variants": [
                {
                    "choices": {"PREORDER PAYMENTS OPTIONS*": "ANTICIPO/SALDO"},
                    "priceData": {"price": fmt_money(price * 0.30)}
                },
                {
                    "choices": {"PREORDER PAYMENTS OPTIONS*": "PAGAMENTO ANTICIPATO"},
                    "priceData": {"price": fmt_money(price * 0.95)}
                }
            ]
        }
        req("PATCH", f"/stores/v1/products/{product_id}/variants", variant_body, ok=(200,))
    except Exception as e:
        raise RuntimeError(f"PATCH /products/{product_id}/variants fallita: {e}")

    # Aggiungi a categoria se presente
    if categoria:
        coll_key = categoria.strip().lower()
        coll_id = collections_map.get(coll_key)
        if coll_id:
            try:
                body = {"productIds": [product_id]}
                req("POST", f"/stores/v1/collections/{coll_id}/productIds", body, ok=(200,204))
                log(f"[INFO] Assegnato a collection '{categoria}'")
            except Exception as e:
                log(f"[WARN] Assegnazione categoria '{categoria}' fallita: {e}")
        else:
            log(f"[WARN] Collection '{categoria}' non trovata, il prodotto non sarà categorizzato.")

    log(f"[OK] Riga {rownum} '{name}' creato/aggiornato (SKU={sku})")

# ---------- main

def precheck():
    if not WIX_API_KEY or not WIX_SITE_ID:
        raise RuntimeError("WIX_API_KEY o WIX_SITE_ID mancanti (secrets).")
    # semplice ping lettura prodotti
    try:
        req("POST", "/stores/v1/products/query", {"query": {"paging": {"limit": 1}}}, ok=(200,))
        log("[PRECHECK] API ok.")
    except Exception as e:
        log(f"[WARN] Precheck prodotti con v1/query fallito: {e}")

def main():
    if len(sys.argv) > 1 and sys.argv[1].strip():
        csv_path = sys.argv[1].strip()
    else:
        csv_path = "input/template_preordini_v7.csv"

    log("[PRECHECK] API ok. Inizio…")
    # carica categorie
    try:
        collections = get_collections_map()
        if collections:
            names = ", ".join(sorted(list(collections.keys()))[:20])
            log(f"[INFO] Categorie caricate: {names}")
        else:
            log("[WARN] Nessuna categoria recuperata.")
    except Exception as e:
        log(f"[WARN] Lettura categorie fallita: {e}")
        collections = {}

    log(f"[INFO] CSV: {csv_path}")
    total_ok = 0
    total_err = 0

    for rownum, row in read_csv(csv_path):
        try:
            create_or_update_product(rownum, row, collections)
            total_ok += 1
        except Exception as e:
            name = (row.get("nome_articolo") or "").strip()
            log(f"[ERRORE] Riga {rownum} '{name}': {e}")
            total_err += 1

    if total_ok > 0 and total_err == 0:
        log(f"[FINE] Ok: {total_ok}")
        sys.exit(0)
    elif total_ok > 0:
        log(f"[FINE] Ok: {total_ok}  Errori: {total_err}")
        sys.exit(0)
    else:
        log("[ERRORE] Nessun prodotto creato/aggiornato.")
        sys.exit(2)

if __name__ == "__main__":
    main()
