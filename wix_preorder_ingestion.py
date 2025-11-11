#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import argparse, csv, os, sys, time, json, re
from urllib.parse import urlparse
import requests
from bs4 import BeautifulSoup
from slugify import slugify

API_KEY = os.environ.get("WIX_API_KEY", "").strip()
SITE_ID = os.environ.get("WIX_SITE_ID", "").strip()

BASE_V1 = "https://www.wixapis.com/stores/v1"
CURRENCY = "EUR"
TIMEOUT = 30

# ---------- http ----------
def _headers():
    if not API_KEY or not SITE_ID:
        print("[ERRORE] Mancano WIX_API_KEY o WIX_SITE_ID nei secrets.", file=sys.stderr)
        sys.exit(2)
    return {
        "Authorization": API_KEY,
        "wix-site-id": SITE_ID,
        "Content-Type": "application/json",
    }

def http(method, url, payload=None):
    r = requests.request(method, url, timeout=TIMEOUT, headers=_headers(),
                         data=json.dumps(payload) if payload is not None else None)
    if r.status_code >= 400:
        raise RuntimeError(f"{method} {url} failed {r.status_code}: {r.text}")
    return r.json() if r.text else {}

# ---------- csv ----------
def parse_csv(path):
    with open(path, "r", encoding="utf-8-sig", newline="") as f:
        sample = f.read(8192)
    delimiter = ";" if sample.count(";") >= sample.count(",") else ","
    rows = []
    with open(path, "r", encoding="utf-8-sig", newline="") as f:
        rdr = csv.DictReader(f, delimiter=delimiter)
        if rdr.fieldnames is None:
            raise RuntimeError("CSV senza header")
        for i, r in enumerate(rdr, start=2):
            row = { (k or "").strip().lower(): (v or "").strip() for k,v in r.items() }
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
    if not url:
        return ""
    try:
        html = requests.get(url, timeout=30).text
        soup = BeautifulSoup(html, "lxml")
        meta = soup.select_one('meta[property="og:description"]')
        if meta and meta.get("content"):
            return meta["content"].strip()
        candidates = [t.get_text(" ", strip=True) for t in soup.select("div,section,p,article")]
        candidates = [c for c in candidates if len(c) > 120]
        candidates.sort(key=len, reverse=True)
        if candidates:
            return candidates[0][:7900]
    except Exception:
        pass
    return ""

# ---------- collections ----------
def find_collection_by_name(name):
    if not name:
        return None
    url = f"{BASE_V1}/collections/query"
    cursor = None
    wanted = name.strip().lower()
    while True:
        body = {"query": {"paging": {"limit": 100}}}
        if cursor:
            body["query"]["cursorPaging"] = {"cursor": cursor}
        data = http("POST", url, body)
        items = data.get("collections", []) or data.get("items", [])
        for c in items:
            if c.get("name","").strip().lower() == wanted:
                return c.get("id")
        cursor = data.get("pagingMetadata", {}).get("cursors", {}).get("next")
        if not cursor:
            return None

def add_product_to_collection(product_id, collection_id):
    if not collection_id:
        return
    url = f"{BASE_V1}/collections/{collection_id}/products/add"
    http("POST", url, {"productIds": [product_id]})

# ---------- create product ----------
def build_product_payload_v1(row):
    full_name = row.get("nome_articolo","").strip()
    # Wix V1: max 80 caratteri per name
    name80 = full_name[:80]

    # prezzo
    try:
        price = float(row.get("prezzo_eur","0").replace(",", "."))
    except:
        price = 0.0

    sku = row.get("sku","").strip()
    brand = row.get("brand","").strip() or None
    url_distrib = row.get("url_produttore") or row.get("link_url_distributore") or row.get("url") or ""
    descr_raw = fetch_description(url_distrib).strip() or row.get("descrizione","").strip()

    # opzionali dal CSV se già presenti con i nomi del tuo v7
    eta = row.get("eta") or row.get("eta_trimestre") or row.get("eta_raw") or ""
    deadline = row.get("preorder_scadenza") or row.get("deadline_preordine") or row.get("deadline_raw") or ""

    intro_parts = []
    if eta: intro_parts.append(f"ETA: {eta}")
    if deadline: intro_parts.append(f"Deadline preordine: {deadline}")
    intro = ("**" + " | ".join(intro_parts) + "**\n\n") if intro_parts else ""

    desc_html = "<div><p>" + (intro + (descr_raw or "")).replace("\n","<br>") + "</p></div>"

    # slug: uso il nome tagliato + suffisso sku se presente per univocità
    base_slug = slugify(name80)
    if sku:
        base_slug = f"{base_slug}-{slugify(sku)}"

    product = {
        "name": name80,
        "slug": base_slug,
        "visible": True,
        "productType": "physical",
        "description": desc_html,
        "priceData": {"currency": CURRENCY, "price": round(price, 2)},
        "manageVariants": True,
        "productOptions": [
            {
                "name": "PREORDER PAYMENTS OPTIONS*",
                "choices": [
                    {"value": "ANTICIPO/SALDO", "description": "Pagamento con acconto 30% e saldo alla consegna"},
                    {"value": "PAGAMENTO ANTICIPATO", "description": "Pagamento anticipato con sconto 5%"}
                ],
            }
        ],
        "ribbon": "PREORDER",
        "seoData": {
            "title": full_name[:300],
            "description": (descr_raw[:300] if descr_raw else ""),
        },
    }
    if brand:
        product["brand"] = brand
    return product, sku, price

def create_product_v1(product):
    url = f"{BASE_V1}/products"
    try:
        data = http("POST", url, {"product": product})
        return data.get("id")
    except RuntimeError as e:
        msg = str(e)
        if "Expected an object" in msg:
            data = http("POST", url, product)
            return data.get("id")
        raise

def patch_variants_v1(product_id, sku_base, price_eur):
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

    # precheck soft
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

            # categoria dal CSV (accetta 'categoria' o 'categoria_wix')
            cat_name = (row.get("categoria") or row.get("categoria_wix") or "").strip()
            if cat_name:
                try:
                    coll_id = find_collection_by_name(cat_name)
                    if coll_id:
                        add_product_to_collection(product_id, coll_id)
                        print(f"[INFO] Assegnato a collection '{cat_name}'")
                    else:
                        print(f"[WARN] Collection '{cat_name}' non trovata, il prodotto non sarà categorizzato.")
                except Exception as e:
                    print(f"[WARN] Collection assign: {e}")

            # varianti con prezzi corretti
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
