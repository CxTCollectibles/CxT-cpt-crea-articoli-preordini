#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import csv
import html
import json
import os
import re
import sys
import time
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

def log(msg: str):
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
    if r.text and r.text.strip():
        try:
            return r.json()
        except Exception:
            return {}
    return {}

# ---------- CSV mapping (tollerante)

HEADER_ALIASES: Dict[str, list] = {
    "nome_articolo": ["nome_articolo","titolo","titolo_articolo","name","product_name"],
    "prezzo_eur": ["prezzo_eur","prezzo","price","price_eur","prezzo_ivato"],
    "sku": ["sku","codice_sku","codice","codice_prodotto","product_code"],
    "brand": ["brand","marchio","marca"],
    "categoria": ["categoria","collezione","collection","categoria_wix"],
    "descrizione": ["descrizione","description","descrizione_en","descrizione_it","desc"],
    "preorder_deadline": ["preorder_deadline","scadenza_preordine","deadline_preordine","deadline","preorder_scadenza"],
    "eta": ["eta","uscita_prevista","release_eta","data_uscita","eta_release"],
}
REQUIRED_MIN = ["nome_articolo","prezzo_eur","sku"]

def pick_header(fieldnames, wanted_list):
    fset = { (f or "").strip().lower(): f for f in fieldnames if f is not None }
    for cand in wanted_list:
        key = cand.lower()
        if key in fset:
            return fset[key]
    return None

def read_csv(path: str):
    with open(path, "r", encoding="utf-8-sig", newline="") as fh:
        rdr = csv.DictReader(fh, delimiter=';')
        if not rdr.fieldnames:
            raise RuntimeError("CSV senza intestazioni.")
        selected = {}
        for canon, aliases in HEADER_ALIASES.items():
            selected[canon] = pick_header(rdr.fieldnames, aliases)
        missing_min = [c for c in REQUIRED_MIN if not selected.get(c)]
        if missing_min:
            raise RuntimeError(f"CSV mancano colonne minime: {missing_min}")

        log("[INFO] Mappatura colonne:")
        for k, v in selected.items():
            log(f"  - {k}: {v or '(assente)'}")

        for i, raw in enumerate(rdr, start=2):
            row = {}
            for canon in HEADER_ALIASES.keys():
                col = selected.get(canon)
                row[canon] = (raw.get(col) if col else "") or ""
            yield i, row

# ---------- formattazioni

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

def fmt_money(v) -> float:
    try:
        return round(float(v), 2)
    except Exception:
        return 0.0

def eta_to_quarters(eta_text: str) -> str:
    if not eta_text:
        return ""
    mesi_q = {1:"Q1",2:"Q1",3:"Q1",4:"Q2",5:"Q2",6:"Q2",7:"Q3",8:"Q3",9:"Q3",10:"Q4",11:"Q4",12:"Q4"}
    m_map = {'gen':'1','feb':'2','mar':'3','apr':'4','mag':'5','giu':'6','lug':'7','ago':'8','set':'9','ott':'10','nov':'11','dic':'12'}
    t = eta_text.lower()
    for k, v in m_map.items():
        if k in t:
            m = int(v)
            q = mesi_q[m]
            nxt = {"Q1":"Q2","Q2":"Q3","Q3":"Q4","Q4":"Q1"}[q]
            return f"{q} - {nxt}"
    return eta_text

def build_description(deadline: str, eta: str, descr: str) -> str:
    parts = []
    if deadline:
        parts.append(f"<p><strong>PREORDER DEADLINE:</strong> {html.escape(deadline)}</p>")
    if eta:
        parts.append(f"<p><strong>ETA:</strong> {html.escape(eta_to_quarters(eta))}</p>")
    if parts:
        parts.append("<p>&nbsp;</p>")  # separatore linea vuota
    if descr:
        safe = html.escape(descr)
        safe = safe.replace("\r\n","\n").replace("\r","\n").replace("\n","<br>")
        parts.append(f"<p>{safe}</p>")
    return "<div>" + "".join(parts) + "</div>"

# ---------- categorie

def get_collections_map() -> Dict[str, str]:
    # tenta GET, se fallisce usa POST /query
    try:
        res = req("GET", "/stores/v1/collections?limit=100", None, ok=(200,))
        items = res.get("collections", []) or res.get("items", []) or []
    except Exception:
        body = {"query": {"paging": {"limit": 100}}}
        res = req("POST", "/stores/v1/collections/query", body, ok=(200,))
        items = res.get("collections", []) or res.get("items", []) or []
    mp = {}
    for c in items:
        name = (c.get("name") or "").strip().lower()
        cid = c.get("id")
        if name and cid:
            mp[name] = cid
    return mp

def find_product_by_sku(sku: str) -> Optional[str]:
    if not sku:
        return None
    # filtro semplice
    try:
        body = {"query": {"filter": {"sku": sku}, "paging": {"limit": 1}}}
        res = req("POST", "/stores/v1/products/query", body, ok=(200,))
        items = res.get("products", []) or res.get("items", [])
        if items:
            return items[0].get("id")
    except Exception:
        pass
    # variante con $eq
    try:
        body = {"query": {"filter": {"sku": {"$eq": sku}}, "paging": {"limit": 1}}}
        res = req("POST", "/stores/v1/products/query", body, ok=(200,))
        items = res.get("products", []) or res.get("items", [])
        if items:
            return items[0].get("id")
    except Exception:
        pass
    return None

# ---------- brand

def normalize_brand(brand_name: str) -> Optional[str]:
    b = (brand_name or "").strip()
    return b if b else None

# ---------- core create/update

def create_or_update_product(rownum: int, row: Dict[str, str], collections_map: Dict[str,str]):
    name_raw = (row.get("nome_articolo") or "").strip()
    price = fmt_money(row.get("prezzo_eur") or 0)
    if not name_raw or price <= 0:
        log(f"[SKIP] Riga {rownum}: nome_articolo o prezzo_eur mancante/non valido.")
        return

    sku = (row.get("sku") or "").strip()
    brand = normalize_brand(row.get("brand") or "")
    categoria = (row.get("categoria") or "").strip()
    descr = (row.get("descrizione") or "").strip()
    deadline = (row.get("preorder_deadline") or "").strip()
    eta = (row.get("eta") or "").strip()

    name = sanitize_name(name_raw)
    slug = mk_slug(name, sku)
    description_html = build_description(deadline, eta, descr)

    product_core = {
        "name": name,
        "slug": slug,
        "productType": "physical",     # stringa, non numero
        "priceData": {"price": price},
        "sku": sku,
        "description": description_html,
        "ribbon": "PREORDER",
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
    if brand:
        product_core["brand"] = brand  # stringa semplice per v1

    product_payload = {"product": product_core}

    existing_id = find_product_by_sku(sku)

    if existing_id:
        log(f"[UPD] Riga {rownum}: aggiorno '{name}' (SKU={sku})")
        try:
            req("PATCH", f"/stores/v1/products/{existing_id}", product_payload, ok=(200,))
        except Exception as e:
            raise RuntimeError(f"PATCH /products/{existing_id} fallita: {e}")
        product_id = existing_id
    else:
        log(f"[NEW] {name} (SKU={sku})")
        try:
            res = req("POST", "/stores/v1/products", product_payload, ok=(200,))
        except Exception as e:
            raise RuntimeError(f"POST /products fallita: {e}")
        product_id = (res.get("product") or {}).get("id") or res.get("id")
        if not product_id:
            raise RuntimeError("ID prodotto non ricevuto.")

    # Varianti con prezzi specifici
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

    # Categoria (collezione)
    if categoria:
        key = categoria.strip().lower()
        coll_id = collections_map.get(key)
        if coll_id:
            try:
                body = {"productIds": [product_id]}
                # endpoint corretto per aggiungere prodotti alla collezione
                req("POST", f"/stores/v1/collections/{coll_id}/products/add", body, ok=(200,))
                log(f"[INFO] Assegnato a collection '{categoria}'")
            except Exception as e:
                log(f"[WARN] Assegnazione categoria '{categoria}' fallita: {e}")
        else:
            log(f"[WARN] Collection '{categoria}' non trovata, il prodotto non sarà categorizzato.")

    log(f"[OK] Riga {rownum} '{name}' creato/aggiornato (SKU={sku})")

def precheck():
    if not WIX_API_KEY or not WIX_SITE_ID:
        raise RuntimeError("WIX_API_KEY o WIX_SITE_ID mancanti (secrets).")
    try:
        req("POST", "/stores/v1/products/query", {"query": {"paging": {"limit": 1}}}, ok=(200,))
        log("[PRECHECK] API ok.")
    except Exception as e:
        log(f"[WARN] Precheck prodotti con v1/query fallito: {e}")

def main():
    csv_path = sys.argv[1].strip() if len(sys.argv) > 1 and sys.argv[1].strip() else "input/template_preordini_v7.csv"
    log(f"[INFO] CSV: {csv_path}")
    precheck()

    try:
        collections = get_collections_map()
        if collections:
            names = ", ".join(sorted(list(collections.keys())))
            log(f"[INFO] Categorie caricate: {names}")
        else:
            log("[WARN] Nessuna categoria recuperata.")
            collections = {}
    except Exception as e:
        log(f"[WARN] Lettura categorie fallita: {e}")
        collections = {}

    ok_cnt = 0
    err_cnt = 0

    for rownum, row in read_csv(csv_path):
        try:
            create_or_update_product(rownum, row, collections)
            ok_cnt += 1
        except Exception as e:
            name = (row.get("nome_articolo") or "").strip()
            log(f"[ERRORE] Riga {rownum} '{name}': {e}")
            err_cnt += 1

    if ok_cnt > 0 and err_cnt == 0:
        log(f"[FINE] Ok: {ok_cnt}")
        sys.exit(0)
    elif ok_cnt > 0:
        log(f"[FINE] Ok: {ok_cnt}  Errori: {err_cnt}")
        sys.exit(0)
    else:
        log("[ERRORE] Nessun prodotto creato/aggiornato.")
        sys.exit(2)

if __name__ == "__main__":
    main()
