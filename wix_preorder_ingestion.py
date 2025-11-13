#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import csv
import json
import os
import re
import sys
import time
from decimal import Decimal, ROUND_HALF_UP

import requests

BASE = "https://www.wixapis.com"

OPTION_NAME = "PREORDER PAYMENTS OPTIONS*"
CHOICE_DEPOSIT = "ANTICIPO/SALDO"
CHOICE_PREPAID = "PAGAMENTO ANTICIPATO"

CSV_DEFAULT_PATH = "input/template_preordini_v7.csv"

# -------- util --------

def money_to_decimal(val):
    if val is None:
        return None
    s = str(val).strip().replace("€", "").replace(" ", "")
    s = s.replace(",", ".")
    try:
        d = Decimal(s)
    except:
        return None
    q = d.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
    return q

def pct(amount, percent):
    return (amount * Decimal(percent)).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)

def trunc(s, n):
    s = s or ""
    return s if len(s) <= n else s[:n]

def norm(s):
    return re.sub(r"\s+", " ", (s or "").strip()).lower()

def headers():
    auth = os.getenv("WIX_AUTH_HEADER") or os.getenv("WIX_API_KEY")
    site = os.getenv("WIX_SITE_ID")
    if not auth or not site:
        raise RuntimeError("Variabili WIX_AUTH_HEADER (o WIX_API_KEY) e/o WIX_SITE_ID mancanti.")
    return {
        "Authorization": auth,
        "wix-site-id": site,
        "Content-Type": "application/json"
    }

S = requests.Session()

def req(method, path, **kwargs):
    url = BASE + path
    h = headers()
    r = S.request(method, url, headers=h, timeout=30, **kwargs)
    return r

# -------- stores helpers --------

def get_collections():
    # Potrebbero essere 404 se Stores non è abilitato per la chiave: in quel caso log e proseguiamo senza bloccare.
    r = req("GET", "/stores/v1/collections?limit=100")
    if r.status_code != 200:
        print(f"[WARN] Lettura categorie fallita: {r.status_code} {r.text}")
        return {}
    data = r.json()
    out = {}
    for c in data.get("collections", []):
        # alcuni payload usano "id", altri "_id"
        cid = c.get("id") or c.get("_id")
        cname = c.get("name") or ""
        out[norm(cname)] = cid
    if out:
        print("[INFO] Categorie caricate: " + ", ".join(sorted(out.keys())))
    return out

def find_product_by_sku(sku):
    body = {
        "query": {
            "filter": {"sku": str(sku)},
            "paging": {"limit": 1}
        }
    }
    r = req("POST", "/stores/v1/products/query", json=body)
    if r.status_code != 200:
        print(f"[WARN] Query SKU fallita {sku}: {r.status_code} {r.text}")
        return None
    items = r.json().get("items", [])
    return items[0] if items else None

def create_product(payload):
    r = req("POST", "/stores/v1/products", json={"product": payload})
    if r.status_code != 200:
        raise RuntimeError(f"POST /products fallita: {r.status_code} {r.text}")
    return r.json().get("id") or r.json().get("_id") or r.json().get("product", {}).get("id")

def update_product(pid, payload):
    r = req("PATCH", f"/stores/v1/products/{pid}", json={"product": payload})
    if r.status_code != 200:
        raise RuntimeError(f"PATCH /products/{pid} fallita: {r.status_code} {r.text}")

def add_to_collection(collection_id, product_id):
    if not collection_id:
        return False
    body = {"productIds": [product_id]}
    r = req("POST", f"/stores/v1/collections/{collection_id}/products/add", json=body)
    if r.status_code == 200:
        return True
    # smart collection o altro errore
    print(f"[WARN] Assegnazione a collection fallita: {r.status_code} {r.text}")
    return False

def ensure_options_and_manage_variants(pid):
    # Imposta l’opzione di pagamento e abilita la gestione varianti
    payload = {
        "manageVariants": True,
        "productOptions": [
            {
                "name": OPTION_NAME,
                "choices": [
                    {"value": CHOICE_DEPOSIT},
                    {"value": CHOICE_PREPAID}
                ]
            }
        ]
    }
    update_product(pid, payload)

def query_variants(pid):
    # preferisco l'endpoint di query per avere fino a 100 risultati
    body = {"query": {"filter": {}, "paging": {"limit": 100}}}
    r = req("POST", f"/stores/v1/products/{pid}/variants/query", json=body)
    if r.status_code == 200:
        return r.json().get("items", [])
    # fallback GET se la query non esistesse
    r2 = req("GET", f"/stores/v1/products/{pid}/variants?limit=100")
    if r2.status_code == 200:
        return r2.json().get("variants", []) or r2.json().get("items", [])
    raise RuntimeError(f"Query varianti fallita: {r.status_code} {r.text} / fallback {r2.status_code} {r2.text}")

def patch_variants_by_id(pid, updates):
    # updates = [{"id": "...", "priceData": {"price": "12.34"}} , ...]
    r = req("PATCH", f"/stores/v1/products/{pid}/variants", json={"variants": updates})
    if r.status_code != 200:
        raise RuntimeError(f"PATCH /products/{pid}/variants fallita: {r.status_code} {r.text}")

def build_description(preorder_deadline, eta, descr):
    parts = []
    if preorder_deadline:
        parts.append(f"<p><strong>Preorder Deadline:</strong> {preorder_deadline}</p>")
    if eta:
        parts.append(f"<p><strong>ETA:</strong> {eta}</p>")
    # riga vuota tra testata e corpo
    parts.append("<p>&nbsp;</p>")
    if descr:
        safe = descr.replace("\n", "<br>")
        parts.append(f"<div>{safe}</div>")
    return "".join(parts)

def row_to_product_payload(row):
    nome = trunc(row.get("nome_articolo"), 80)
    sku = (row.get("sku") or "").strip()
    brand = (row.get("brand") or "").strip()
    raw_price = money_to_decimal(row.get("prezzo_eur"))
    if not nome or not raw_price or not sku:
        raise RuntimeError("nome_articolo, prezzo_eur, sku sono obbligatori.")

    descr_html = build_description(
        row.get("preorder_scadenza") or row.get("preorder_deadline"),
        row.get("eta"),
        row.get("descrizione")
    )

    payload = {
        "name": nome,
        "sku": sku,
        "brand": brand if brand else None,
        "productType": "physical",
        "priceData": {
            "price": str(raw_price),
            "currency": "EUR"
        },
        "ribbons": ["Preorder"],  # toppa visiva se il toggle "preorder" non è esposto via API
        "description": descr_html,
        "manageVariants": True,
        "productOptions": [
            {
                "name": OPTION_NAME,
                "choices": [
                    {"value": CHOICE_DEPOSIT},
                    {"value": CHOICE_PREPAID}
                ]
            }
        ],
        "visible": True
    }
    return payload, raw_price

def match_collection_id(collections_map, wanted):
    if not wanted:
        return None
    w = norm(wanted)
    if w in collections_map:
        return collections_map[w]
    # match parziale tollerante
    for k, v in collections_map.items():
        if w in k or k in w:
            return v
    return None

def process_csv(csv_path):
    cols = None
    with open(csv_path, "r", encoding="utf-8-sig", newline="") as fh:
        reader = csv.DictReader(fh, delimiter=";")
        cols = [c.strip() for c in reader.fieldnames]
        required = {"nome_articolo","prezzo_eur","sku","brand","categoria","descrizione"}
        missing = [c for c in required if c not in cols]
        if missing:
            raise RuntimeError(f"CSV mancano colonne: {missing}")
        rows = list(reader)
    return rows

def main():
    csv_path = sys.argv[1] if len(sys.argv) > 1 else CSV_DEFAULT_PATH
    print(f"[INFO] CSV: {csv_path}")

    rows = process_csv(csv_path)
    collections = get_collections()

    created, updated, errors = 0, 0, 0

    for idx, row in enumerate(rows, start=2):
        try:
            name_preview = trunc(row.get("nome_articolo"), 80)
            sku = (row.get("sku") or "").strip()
            if len(name_preview) == 80 and (row.get("nome_articolo") and len(row["nome_articolo"]) > 80):
                print("[WARN] Nome > 80 caratteri, troncato.")

            p = find_product_by_sku(sku)
            product_exists = p is not None
            pid = p.get("id") if product_exists else None

            payload, base_price = row_to_product_payload(row)

            if not product_exists:
                pid = create_product(payload)
                created += 1
                print(f"[NEW] {name_preview} (SKU={sku}) id={pid}")
            else:
                update_product(pid, payload)
                updated += 1
                print(f"[UPD] {name_preview} (SKU={sku}) id={pid}")

            # CATEGORIA
            col_name = row.get("categoria")
            col_id = match_collection_id(collections, col_name)
            if col_id:
                ok = add_to_collection(col_id, pid)
                if ok:
                    print(f"[OK] Categoria assegnata: {col_name}")
                else:
                    print(f"[WARN] Categoria '{col_name}' non assegnata (smart o errore).")
            else:
                print(f"[WARN] Categoria '{col_name}' non trovata.")

            # VARIANTI E PREZZI
            # Assicuro che esista l'opzione e che le varianti siano gestite
            ensure_options_and_manage_variants(pid)

            # Recupero varianti generate
            variants = query_variants(pid)
            if not variants:
                raise RuntimeError("Nessuna variante generata.")

            var_deposit_id = None
            var_prepaid_id = None
            for v in variants:
                # "choices" può essere dict {OPTION_NAME: CHOICE}
                choices = v.get("choices") or {}
                # in certi payload choices è una lista di mappe: normalizzo in dict
                if isinstance(choices, list):
                    tmp = {}
                    for ch in choices:
                        # supporta forme diverse
                        k = ch.get("name") or ch.get("option") or ch.get("title")
                        val = ch.get("value") or ch.get("selection") or ch.get("description")
                        if k and val:
                            tmp[k] = val
                    choices = tmp

                pick = choices.get(OPTION_NAME)
                if pick == CHOICE_DEPOSIT:
                    var_deposit_id = v.get("id")
                elif pick == CHOICE_PREPAID:
                    var_prepaid_id = v.get("id")

            if not var_deposit_id or not var_prepaid_id:
                raise RuntimeError("Varianti attese non trovate (controlla nomi esatti di opzione/scelte).")

            price_deposit = pct(base_price, "0.30")
            price_prepaid = pct(base_price, "0.95")

            updates = []
            updates.append({"id": var_deposit_id, "priceData": {"price": str(price_deposit), "currency": "EUR"}})
            updates.append({"id": var_prepaid_id, "priceData": {"price": str(price_prepaid), "currency": "EUR"}})

            patch_variants_by_id(pid, updates)
            print(f"[OK] Prezzi varianti aggiornati: {CHOICE_DEPOSIT}={price_deposit} EUR, {CHOICE_PREPAID}={price_prepaid} EUR")

        except Exception as e:
            errors += 1
            print(f"[ERRORE] Riga {idx} '{trunc(row.get('nome_articolo'),50)}': {e}")

    print(f"[DONE] Creati: {created}, Aggiornati: {updated}, Errori: {errors}")
    if errors:
        sys.exit(2)

if __name__ == "__main__":
    main()
main()
