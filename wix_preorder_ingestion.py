#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import argparse, csv, os, sys, time, json, re, math
from urllib.parse import urlparse
import requests
from bs4 import BeautifulSoup
from slugify import slugify

API_KEY = os.environ.get("WIX_API_KEY", "").strip()
SITE_ID = os.environ.get("WIX_SITE_ID", "").strip()

BASE_V1 = "https://www.wixapis.com/stores/v1"
BASE_V3 = "https://www.wixapis.com/stores/v3"

CURRENCY = "EUR"
TIMEOUT = 30

# ---------- util http ----------
def headers():
    if not API_KEY or not SITE_ID:
        print("[ERRORE] Mancano WIX_API_KEY o WIX_SITE_ID nei secrets.", file=sys.stderr)
        sys.exit(2)
    return {
        "Authorization": API_KEY,
        "wix-site-id": SITE_ID,
        "Content-Type": "application/json",
    }

def http(method, url, payload=None):
    r = requests.request(method, url, timeout=TIMEOUT, headers=headers(),
                         data=json.dumps(payload) if payload is not None else None)
    if r.status_code >= 400:
        raise RuntimeError(f"{method} {url} failed {r.status_code}: {r.text}")
    return r.json() if r.text else {}

# ---------- csv ----------
REQUIRED = ["nome_articolo","prezzo_eur"]
def parse_csv(path):
    # auto delimiter ; oppure , e BOM-friendly
    with open(path, "r", encoding="utf-8-sig", newline="") as f:
        sample = f.read(8192)
    delimiter = ";" if sample.count(";") >= sample.count(",") else ","
    rows = []
    with open(path, "r", encoding="utf-8-sig", newline="") as f:
        rdr = csv.DictReader(f, delimiter=delimiter)
        if rdr.fieldnames is None:
            raise RuntimeError("CSV senza header")
        # normalizza header
        fieldmap = { (h or "").strip().lower(): h for h in rdr.fieldnames }
        # controlli minimi riga per riga
        for i, r in enumerate(rdr, start=2):
            row = { k.strip().lower(): (v or "").strip() for k,v in r.items() }
            if not row.get("nome_articolo") or not row.get("prezzo_eur"):
                print(f"[SKIP] Riga {i}: nome_articolo o prezzo_eur mancante.")
                continue
            rows.append(row)
    if not rows:
        raise RuntimeError("CSV vuoto o non valido.")
    print(f"[INFO] CSV encoding=utf-8-sig delimiter='{delimiter}'")
    return rows

# ---------- descrizione (basic) ----------
def fetch_description(url):
    if not url: return ""
    try:
        html = requests.get(url, timeout=30).text
        soup = BeautifulSoup(html, "lxml")
        # heuristics: prendi blocchi di testo “descrittivi”
        # 1) meta og:description
        meta = soup.select_one('meta[property="og:description"]')
        if meta and meta.get("content"): return meta["content"].strip()
        # 2) div/p più lunghi
        candidates = [t.get_text(" ", strip=True) for t in soup.select("div,section,p,article")]
        candidates = [c for c in candidates if len(c) > 120]
        candidates.sort(key=len, reverse=True)
        if candidates:
            return candidates[0][:7900]  # margine sotto 8000
    except Exception:
        pass
    return ""

# ---------- collections ----------
def find_collection_by_name(name):
    # query paginata (limit <= 100) V1
    cursor = None
    url = f"{BASE_V1}/collections/query"
    while True:
        body = {"query": {"paging": {"limit": 100}}}
        if cursor:
            body["query"]["cursorPaging"] = {"cursor": cursor}
        try:
            data = http("POST", url, body)
        except RuntimeError as e:
            # se capita limite >100, viene corretto sopra; ogni altro 4xx lo propaghiamo
            raise
        colls = data.get("collections", [])
        for c in colls:
            if c.get("name","").strip().lower() == name.strip().lower():
                return c.get("id")
        cursor = data.get("pagingMetadata", {}).get("cursors", {}).get("next")
        if not cursor:
            return None

def add_product_to_collection(product_id, collection_id):
    if not collection_id: return
    url = f"{BASE_V1}/collections/{collection_id}/products/add"
    http("POST", url, {"productIds": [product_id]})

# ---------- create product ----------
def build_product_payload_v1(row):
    name = row.get("nome_articolo","")
    price = float(row.get("prezzo_eur","0").replace(",", "."))
    sku = row.get("sku","").strip()
    brand = row.get("brand","").strip() or None
    peso = row.get("peso_kg","").replace(",",".")
    peso_val = float(peso) if peso else None
    link_distributore = row.get("link_url_distributore","")

    # descrizione: URL -> fallback CSV -> stringa vuota
    descr_raw = fetch_description(link_distributore).strip()
    if not descr_raw:
        descr_raw = row.get("descrizione","").strip()

    # ETA/Deadline opzionali (se nel CSV)
    eta = row.get("eta_trimestre","").strip()  # es. "Q3 - Q4 2026"
    deadline = row.get("deadline_preordine","").strip()  # es. "31/01/2026"
    intro = ""
    if eta or deadline:
        parts = []
        if eta: parts.append(f"ETA: {eta}")
        if deadline: parts.append(f"Deadline preordine: {deadline}")
        intro = "**" + " | ".join(parts) + "**\n\n"

    # evita backslash dentro f-string
    descr_html = "<div><p>" + (intro + descr_raw).replace("\n","<br>") + "</p></div>"

    product = {
        "name": name,
        "slug": slugify(name),
        "visible": True,
        "productType": "physical",
        "description": descr_html,
        "priceData": {"currency": CURRENCY, "price": price},
        "manageVariants": True,
        "productOptions": [
            {
                "name": "PREORDER PAYMENTS OPTIONS*",
                "choices": [
                    {"description": "ANTICIPO/SALDO"},
                    {"description": "PAGAMENTO ANTICIPATO"},
                ],
            }
        ],
        "ribbon": "PREORDER",
        "brand": brand if brand else "",
        "seoData": {
            "title": name,
            "description": (descr_raw[:300] if descr_raw else ""),
        },
    }
    # peso: se presente spostiamolo nei physicalProperties solo dopo creazione (alcuni siti sono schizzinosi)
    return product, sku, price

def create_product_v1(product):
    # alcuni siti richiedono wrapper {"product": {...}}
    url = f"{BASE_V1}/products"
    try:
        data = http("POST", url, {"product": product})
        return data.get("id")
    except RuntimeError as e:
        msg = str(e)
        if "Expected an object" in msg or "unsupported" in msg:
            # riprova senza wrapper
            data = http("POST", url, product)
            return data.get("id")
        raise

def patch_variants_v1(product_id, sku_base, price_eur):
    # calcoli prezzo: AS = 30% del prezzo, PA = prezzo * 95%
    as_price = round(price_eur * 0.30, 2)
    pa_price = round(price_eur * 0.95, 2)
    body = {
        "variants": [
            {
                "choices": {"PREORDER PAYMENTS OPTIONS*": "ANTICIPO/SALDO"},
                "sku": f"{sku_base}-AS" if sku_base else "",
                "visible": True,
                "priceData": {"currency": CURRENCY, "price": as_price},
            },
            {
                "choices": {"PREORDER PAYMENTS OPTIONS*": "PAGAMENTO ANTICIPATO"},
                "sku": f"{sku_base}-PA" if sku_base else "",
                "visible": True,
                "priceData": {"currency": CURRENCY, "price": pa_price},
            },
        ]
    }
    url = f"{BASE_V1}/products/{product_id}/variants"
    http("PATCH", url, body)

# ---------- main ----------
def run(csv_path):
    rows = parse_csv(csv_path)

    # sanity check API e visibilità prodotti (reader v1)
    try:
        probe = http("POST", f"{BASE_V1}/products/query", {"query": {"paging": {"limit": 5}}})
        vis = len([p for p in probe.get("products", []) if p.get("visible")])
        print(f"[PRECHECK] API ok. Prodotti visibili: {vis}")
    except Exception as e:
        print(f"[WARN] Precheck fallito: {e}")

    created = 0
    for idx, row in enumerate(rows, start=2):
        name = row.get("nome_articolo","").strip()
        print(f"[WORK] Riga {idx}: {name}")
        try:
            product, sku, price = build_product_payload_v1(row)
            product_id = create_product_v1(product)

            if not product_id:
                raise RuntimeError("ID prodotto non ricevuto.")

            # assegna alla collection (se presente nel CSV)
            coll_name = row.get("categoria_wix","").strip()
            if coll_name:
                try:
                    coll_id = find_collection_by_name(coll_name)
                    if coll_id:
                        add_product_to_collection(product_id, coll_id)
                        print(f"[INFO] Assegnato a collection '{coll_name}'")
                    else:
                        print(f"[WARN] Collection '{coll_name}' non trovata, il prodotto non sarà categorizzato.")
                except Exception as e:
                    print(f"[WARN] Collection assign: {e}")

            # patch varianti con prezzi AS/PA
            try:
                patch_variants_v1(product_id, sku, price)
            except Exception as e:
                print(f"[WARN] Patch varianti fallita: {e}")

            print(f"[OK] Riga {idx} creato '{name}'")
            created += 1

        except Exception as e:
            print(f"[ERRORE] Riga {idx} '{name}': {e}")

    if created == 0:
        print("[ERRORE] Nessun prodotto creato.")
        sys.exit(2)
    print(f"[FINE] Prodotti creati: {created}")

if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--csv", required=True, help="Percorso file CSV, es. input/template_preordini_v7.csv")
    args = ap.parse_args()
    run(args.csv)
