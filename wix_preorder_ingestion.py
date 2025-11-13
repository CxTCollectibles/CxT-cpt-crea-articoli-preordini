#!/usr/bin/env python3
import os
import sys
import csv
import json
import time
import math
from datetime import datetime
from typing import Dict, Any, List, Optional

import requests

BASE = "https://www.wixapis.com"

# ===== Utilities =====

def env(name: str) -> str:
    v = os.environ.get(name)
    if not v:
        raise RuntimeError(f"Variabile {name} mancante")
    return v

def headers() -> Dict[str, str]:
    return {
        "Authorization": env("WIX_API_KEY"),
        "wix-site-id": env("WIX_SITE_ID"),
        "Content-Type": "application/json"
    }

def req(method: str, path: str, **kw) -> requests.Response:
    url = f"{BASE}{path}"
    r = requests.request(method, url, headers=headers(), timeout=30, **kw)
    return r

def safe_float(x: str) -> float:
    x = (x or "").strip().replace(",", ".")
    try:
        return float(x)
    except Exception:
        return 0.0

def truncate_name(name: str) -> str:
    name = (name or "").strip()
    return name[:80] if len(name) > 80 else name

def fmt_deadline_eta(deadline: str, eta: str) -> str:
    deadline = (deadline or "").strip()
    eta = (eta or "").strip()
    lines = []
    if deadline:
        lines.append(f"Preorder Deadline: {deadline}")
    if eta:
        lines.append(f"ETA: {eta}")
    if lines:
        # Riga vuota dopo intestazione, poi descrizione
        return "\n".join(lines) + "\n\n"
    return ""

def round2(x: float) -> float:
    return float(f"{x:.2f}")

def catalog_version() -> str:
    # Ritorna "V3" se raggiungibile, altrimenti "UNKNOWN"
    r = req("GET", "/stores/v3/provision/version")
    if r.status_code == 200:
        # Alcuni ambienti tornano testo semplice "V3_CATALOG" o JSON
        try:
            body = r.json()
            # Possibili chiavi: {"version":"V3_CATALOG"} oppure simile
            ver = (body.get("version") or body.get("catalogVersion") or "").upper()
        except Exception:
            ver = (r.text or "").upper()
        if "V3" in ver:
            return "V3"
    return "UNKNOWN"

def load_collections() -> Dict[str, str]:
    """
    Ritorna mappa {nome_lower: id_collection} usando endpoint storico 'stores/v1/collections'.
    Anche con Catalog V3, le collezioni si gestiscono via V1 (documentazione Wix).
    """
    out = {}
    cursor = None
    while True:
        qp = "?limit=50" + (f"&cursor={cursor}" if cursor else "")
        r = req("GET", f"/stores/v1/collections{qp}")
        if r.status_code == 404:
            # Alcuni siti possono limitare le collezioni o l'app non installata
            print("[WARN] Lettura categorie/collections 404. Proseguo senza categorizzare.")
            return out
        r.raise_for_status()
        body = r.json()
        for c in body.get("collections", []):
            name = (c.get("name") or "").strip().lower()
            cid = c.get("id")
            if name and cid:
                out[name] = cid
        cursor = body.get("nextCursor")
        if not cursor:
            break
    return out

def pick_collection_id(collections: Dict[str, str], categoria_raw: str) -> Optional[str]:
    if not categoria_raw:
        return None
    key = categoria_raw.strip().lower()
    # match diretto
    if key in collections:
        return collections[key]
    # fallback: match "soft" per nomi simili
    for k, cid in collections.items():
        if key in k or k in key:
            return cid
    return None

def find_product_by_sku_v3(sku: str) -> Optional[str]:
    """
    Query V3 per SKU. Schema delle query V3: filter su sku/ skus.
    Formiamo un filtro tollerante.
    """
    payloads = [
        {"query": {"filter": {"sku": sku}, "paging": {"limit": 1}}},
        {"query": {"filter": {"skus": [sku]}, "paging": {"limit": 1}}}
    ]
    for p in payloads:
        r = req("POST", "/stores/v3/products/query", json=p)
        if r.status_code == 404:
            # Se proprio non esiste endpoint (improbabile), molla
            return None
        if r.status_code == 400:
            continue
        r.raise_for_status()
        items = r.json().get("products", [])
        if items:
            return items[0].get("id")
    return None

def create_or_update_product_v3(row: Dict[str, str], collections_map: Dict[str, str]) -> None:
    name = truncate_name(row.get("nome_articolo", ""))
    price = round2(safe_float(row.get("prezzo_eur", "0")))
    sku = (row.get("sku") or "").strip()
    brand = (row.get("brand") or "").strip()
    categoria = row.get("categoria") or ""
    descrizione = row.get("descrizione") or ""
    deadline = row.get("preorder_scadenza") or ""
    eta = row.get("eta") or ""

    header = fmt_deadline_eta(deadline, eta)
    full_desc = header + descrizione.strip()

    if not sku:
        print(f"[ERRORE] '{name}': SKU mancante, salto.")
        return

    # Calcolo prezzi varianti
    prezzo_anticipo = round2(price * 0.30)  # 30%
    prezzo_full_sconto = round2(price * 0.95)  # -5%

    # Trovare prodotto esistente per SKU
    pid = find_product_by_sku_v3(sku)

    product_obj = {
        "name": name,
        "brand": brand or None,
        # V3: se gestiamo varianti, il prezzo di base può essere opzionale,
        # ma impostarlo non fa male come "prezzo listino".
        "priceData": {"price": price, "currency": "EUR"},
        "manageVariants": True,
        "productOptions": [
            {
                "name": "PREORDER PAYMENTS OPTIONS*",
                "optionType": "drop_down",
                "choices": [
                    {"value": "ANTICIPO/SALDO"},
                    {"value": "PAGAMENTO ANTICIPATO"}
                ]
            }
        ],
        "description": full_desc,
        # imposto sku a livello variante; metto comunque uno sku "base"
        "sku": sku
    }

    variants = [
        {
            "choices": {"PREORDER PAYMENTS OPTIONS*": "ANTICIPO/SALDO"},
            "sku": f"{sku}-DEP",
            "priceData": {"price": prezzo_anticipo, "currency": "EUR"}
        },
        {
            "choices": {"PREORDER PAYMENTS OPTIONS*": "PAGAMENTO ANTICIPATO"},
            "sku": f"{sku}-FULL",
            "priceData": {"price": prezzo_full_sconto, "currency": "EUR"}
        }
    ]

    if pid:
        # UPDATE
        print(f"[UPD] {name} (SKU={sku}) id={pid}")
        # 1) Update product body (senza varianti)
        r = req("PATCH", f"/stores/v3/products/{pid}", json={"product": product_obj})
        if r.status_code not in (200, 204):
            try:
                print(f"[WARN] Update product fallita: {r.status_code} {r.text}")
            except Exception:
                print(f"[WARN] Update product fallita: {r.status_code}")
        # 2) Sostituisci l’elenco varianti
        # In V3 c’è l’endpoint variants dedicato
        rv = req("PUT", f"/stores/v3/products/{pid}/variants", json={"variants": variants})
        if rv.status_code not in (200, 204):
            print(f"[WARN] Update varianti fallita: {rv.status_code} {rv.text}")
        product_id = pid
    else:
        # CREATE
        payload = {
            "product": product_obj,
            "variants": variants  # V3 consente create con varianti insieme
        }
        r = req("POST", "/stores/v3/products", json=payload)
        if r.status_code == 400 and "sku is not unique" in (r.text or "").lower():
            # race condition: qualcuno l’ha appena creato → ritenta come update
            pid2 = find_product_by_sku_v3(sku)
            if not pid2:
                print(f"[ERRORE] '{name}': SKU duplicato ma non trovo il prodotto.")
                return
            print(f"[INFO] SKU duplicato, passo ad update id={pid2}")
            r2 = req("PATCH", f"/stores/v3/products/{pid2}", json={"product": product_obj})
            if r2.status_code not in (200, 204):
                print(f"[ERRORE] Update fallita: {r2.status_code} {r2.text}")
                return
            rv = req("PUT", f"/stores/v3/products/{pid2}/variants", json={"variants": variants})
            if rv.status_code not in (200, 204):
                print(f"[WARN] Update varianti fallita: {rv.status_code} {rv.text}")
            product_id = pid2
        else:
            r.raise_for_status()
            product_id = r.json().get("product", {}).get("id")

    # Categoria/Collection
    if categoria:
        collections_id = pick_collection_id(collections_map, categoria)
        if collections_id and product_id:
            # API storica per agganciare prodotti a una collection
            body = {"productIds": [product_id]}
            c = req("POST", f"/stores/v1/collections/{collections_id}/productIds", json=body)
            if c.status_code in (200, 204):
                print(f"[OK] Categoria '{categoria}' agganciata.")
            else:
                print(f"[WARN] Aggancio categoria fallito: {c.status_code} {c.text}")
        else:
            print(f"[WARN] Categoria '{categoria}' non trovata tra le collections.")

def read_csv_rows(csv_path: str):
    required = ["nome_articolo", "prezzo_eur", "sku", "brand", "categoria", "descrizione", "preorder_scadenza", "eta"]
    with open(csv_path, "r", encoding="utf-8-sig", newline="") as fh:
        r = csv.DictReader(fh, delimiter=";")
        # Controllo colonne
        miss = [c for c in required if c not in r.fieldnames]
        if miss:
            raise RuntimeError(f"CSV mancano colonne: {miss}")
        for i, row in enumerate(r, start=2):
            yield i, row

def main():
    csv_path = sys.argv[1] if len(sys.argv) > 1 else "input/template_preordini_v7.csv"
    print(f"[INFO] CSV: {csv_path}")
    ver = catalog_version()
    if ver != "V3":
        print(f"[WARN] Catalog version non confermata V3 (risultato: {ver}). Procedo comunque con V3 per prodotti, V1 per collections.")
    # Carico le collections (categorie)
    try:
        collections_map = load_collections()
        if collections_map:
            print("[INFO] Collections disponibili:", ", ".join(sorted(collections_map.keys())))
        else:
            print("[WARN] Nessuna collection caricata (o 404).")
    except Exception as e:
        print(f"[WARN] Lettura collections fallita: {e}")
        collections_map = {}

    created, updated, errors = 0, 0, 0
    for rownum, row in read_csv_rows(csv_path):
        name = truncate_name(row.get("nome_articolo", ""))
        sku = (row.get("sku") or "").strip()
        print(f"[WORK] Riga {rownum}: {name} (SKU={sku})")
        try:
            before = find_product_by_sku_v3(sku)
            create_or_update_product_v3(row, collections_map)
            after = find_product_by_sku_v3(sku)
            if before and after:
                updated += 1
            else:
                created += 1
        except requests.HTTPError as he:
            errors += 1
            print(f"[ERRORE] Riga {rownum} '{name}': {he.response.status_code} {he.response.text}")
        except Exception as ex:
            errors += 1
            print(f"[ERRORE] Riga {rownum} '{name}': {ex}")

    print(f"[DONE] Creati: {created}, Aggiornati: {updated}, Errori: {errors}")
    if errors:
        sys.exit(2)

if __name__ == "__main__":
    main()
