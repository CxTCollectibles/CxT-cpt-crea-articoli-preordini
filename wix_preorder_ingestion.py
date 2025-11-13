#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import csv
import json
import os
import re
import sys
import time
import math
from typing import Dict, Any, List, Optional
import requests
from unidecode import unidecode
from datetime import datetime

WIX_BASE = "https://www.wixapis.com"

API_KEY = os.getenv("WIX_API_KEY", "").strip()
SITE_ID = os.getenv("WIX_SITE_ID", "").strip()

CSV_DEFAULT = "input/template_preordini_v7.csv"

# --- Helper di log -----------------------------------------------------------
def log(msg: str):
    print(msg, flush=True)

def warn(msg: str):
    print(f"[WARN] {msg}", flush=True)

def err(msg: str):
    print(f"[ERRORE] {msg}", flush=True)

# --- HTTP --------------------------------------------------------------------
def headers() -> Dict[str, str]:
    if not API_KEY or not SITE_ID:
        raise RuntimeError("WIX_API_KEY e/o WIX_SITE_ID mancanti (usa i secrets).")
    return {
        "Authorization": API_KEY,           # API Key: formato 'raw', NON 'Bearer'
        "wix-site-id": SITE_ID,             # deve essere il metaSiteId
        "Content-Type": "application/json"
    }

def req(method: str, path: str, body: Optional[Dict[str, Any]] = None, ok=(200, 201)):
    url = f"{WIX_BASE}{path}"
    r = requests.request(method, url, headers=headers(), data=json.dumps(body) if body is not None else None, timeout=30)
    if r.status_code not in ok:
        raise requests.HTTPError(f"{r.status_code} {r.text}".strip(), response=r)
    if r.text and r.headers.get("Content-Type", "").startswith("application/json"):
        return r.json()
    return None

# --- Utility dati ------------------------------------------------------------
def norm(s: str) -> str:
    return unidecode((s or "").strip().lower())

def slugify(name: str) -> str:
    s = unidecode(name).strip().lower()
    s = re.sub(r"[^a-z0-9\- ]+", "", s)
    s = re.sub(r"\s+", "-", s)
    return s[:80] if len(s) > 80 else s

def parse_price(v: str) -> float:
    if v is None:
        return 0.0
    s = str(v).replace("€", "").replace(",", ".").strip()
    try:
        return round(float(s), 2)
    except:
        return 0.0

def trunc_name(name: str) -> str:
    if len(name) > 80:
        warn("Nome > 80 caratteri, troncato.")
    return name[:80]

def html_description(pre_deadline: str, eta: str, descr: str) -> str:
    # costruisco righe senza backslash dentro f-string
    top_lines = []
    if pre_deadline:
        top_lines.append(f"Preorder Deadline: {pre_deadline}")
    if eta:
        top_lines.append(f"ETA: {eta}")
    # riga vuota di separazione
    if descr:
        body = descr.replace("\r\n", "\n").replace("\r", "\n")
    else:
        body = ""
    # HTML semplice: 2 <p> di testa, 1 <p> vuoto, poi testo con <br>
    body_html = body.replace("\n", "<br>")
    parts = []
    for i, line in enumerate(top_lines):
        parts.append(f"<p>{line}</p>")
    parts.append("<p>&nbsp;</p>")
    if body_html:
        parts.append(f"<p>{body_html}</p>")
    return "<div>" + "".join(parts) + "</div>"

# --- Stores: Collections (categorie) -----------------------------------------
def get_collections_map() -> Dict[str, str]:
    """
    Ritorna mappa nome-normalizzato -> collectionId
    Se l'endpoint non è disponibile per permessi, ritorna dict vuoto.
    """
    try:
        res = req("GET", "/stores/v1/collections?limit=100")
        items = (res or {}).get("collections", []) if isinstance(res, dict) else []
        if not items:
            warn("Nessuna collection caricata (o 404).")
            return {}
        names = [c.get("name", "") for c in items]
        log("[INFO] Categorie caricate: " + ", ".join(norm(n) for n in names if n))
        return {norm(c.get("name", "")): c.get("id") for c in items if c.get("id")}
    except requests.HTTPError as e:
        warn(f"Lettura categorie fallita: {e}")
        return {}

def add_to_collection(collection_id: str, product_id: str):
    try:
        body = {"productIds": [product_id]}
        # v1 add products
        req("POST", f"/stores/v1/collections/{collection_id}/products/add", body, ok=(200, 201, 204))
    except requests.HTTPError as e:
        warn(f"Add to collection fallita: {e}")

# --- Stores: Products v1 -----------------------------------------------------
def query_product_by_sku(sku: str) -> Optional[Dict[str, Any]]:
    """
    Prova /stores/v1/products/query con filtro stringa.
    Se non va, prova fallback scan GET paginato (best-effort).
    """
    # Tentativo 1: query
    try:
        body = {
            "query": {
                "filter": f"sku eq \"{sku}\"",
                "paging": {"limit": 50}
            }
        }
        res = req("POST", "/stores/v1/products/query", body)
        items = (res or {}).get("products", [])
        if items:
            return items[0]
    except requests.HTTPError as e:
        warn(f"Query SKU fallita {sku}: {e}")

    # Tentativo 2: scan GET paginato (fino a 5 pagine)
    try:
        cursor = None
        pages = 0
        while pages < 5:
            path = "/stores/v1/products?limit=50"
            if cursor:
                path += f"&cursor={cursor}"
            res = req("GET", path)
            products = (res or {}).get("products", [])
            for p in products:
                if p.get("sku") == sku:
                    return p
            cursor = (res or {}).get("cursor")
            if not cursor:
                break
            pages += 1
    except requests.HTTPError as e:
        warn(f"Scan prodotti fallita: {e}")
    return None

def create_product(payload: Dict[str, Any]) -> str:
    res = req("POST", "/stores/v1/products", payload, ok=(200, 201))
    pid = (res or {}).get("id") or (res or {}).get("product", {}).get("id")
    if not pid:
        raise RuntimeError("ID prodotto non ricevuto.")
    return pid

def update_product(pid: str, payload: Dict[str, Any]):
    req("PATCH", f"/stores/v1/products/{pid}", payload, ok=(200, 201))

def patch_variants(pid: str, variants: List[Dict[str, Any]]):
    body = {"variants": variants}
    req("PATCH", f"/stores/v1/products/{pid}/variants", body, ok=(200, 201))

# --- CSV ---------------------------------------------------------------------
def read_csv(path: str):
    with open(path, "r", encoding="utf-8-sig", newline="") as fh:
        sniff = fh.read(4096)
        fh.seek(0)
        # usiamo ; come da tuo template
        reader = csv.DictReader(fh, delimiter=';')
        cols = [c.strip() for c in reader.fieldnames or []]
        colmap = {
            "nome_articolo": _find_col(cols, ["nome_articolo", "titolo", "name"]),
            "prezzo_eur": _find_col(cols, ["prezzo_eur", "prezzo", "price"]),
            "sku": _find_col(cols, ["sku"]),
            "brand": _find_col(cols, ["brand", "marca"]),
            "categoria": _find_col(cols, ["categoria", "collection", "collezione"]),
            "descrizione": _find_col(cols, ["descrizione", "description"]),
            "preorder_scadenza": _find_col(cols, ["preorder_scadenza", "preorder_deadline", "deadline"]),
            "eta": _find_col(cols, ["eta", "arrivo", "release"])
        }
        missing = [k for k,v in colmap.items() if v is None and k in ("nome_articolo","prezzo_eur","sku")]
        if missing:
            raise RuntimeError(f"CSV mancano colonne: {missing}")
        log("[INFO] Mappatura colonne:")
        for k,v in colmap.items():
            log(f"  - {k}: {v}")
        for row in reader:
            yield row, colmap

def _find_col(cols: List[str], candidates: List[str]) -> Optional[str]:
    nl = {norm(c): c for c in cols}
    for c in candidates:
        if norm(c) in nl:
            return nl[norm(c)]
    return None

# --- Business rules ----------------------------------------------------------
OPTION_TITLE = "PREORDER PAYMENTS OPTIONS*"
CHOICE_A = "ANTICIPO/SALDO"
CHOICE_P = "PAGAMENTO ANTICIPATO"

def build_product_payload(nome: str, sku: str, brand: str, price: float, descr_html: str) -> Dict[str, Any]:
    name80 = trunc_name(nome)
    slug = slugify(name80)
    payload = {
        "name": name80,
        "slug": slug,
        "sku": sku,
        "productType": "physical",
        "visible": True,
        "priceData": {
            "price": price,
            "currency": "EUR"
        },
        "brand": {"name": brand} if brand else None,
        "description": descr_html,
        # attivo gestione varianti e imposto l'opzione con 2 scelte
        "manageVariants": True,
        "productOptions": [
            {
                "title": OPTION_TITLE,
                "choices": [
                    {"title": CHOICE_A, "description": "Paga ora il 30% come acconto, saldo alla disponibilità."},
                    {"title": CHOICE_P, "description": "Pagamento anticipato con 5% di sconto sul prezzo."}
                ]
            }
        ]
    }
    # rimuovi None
    payload = {k:v for k,v in payload.items() if v is not None}
    return payload

def build_variant_overrides(price: float, sku: str) -> List[Dict[str, Any]]:
    price_anticipo = round(price * 0.30, 2)
    price_prepaid = round(price * 0.95, 2)
    return [
        {
            "choices": {OPTION_TITLE: CHOICE_A},
            "priceData": {"price": price_anticipo, "currency": "EUR"},
            "sku": f"{sku}-A"
        },
        {
            "choices": {OPTION_TITLE: CHOICE_P},
            "priceData": {"price": price_prepaid, "currency": "EUR"},
            "sku": f"{sku}-P"
        }
    ]

# --- Main --------------------------------------------------------------------
def main():
    csv_path = sys.argv[1].strip() if len(sys.argv) > 1 else CSV_DEFAULT
    log(f"[INFO] CSV: {csv_path}")

    # Carico mappa categorie (se permesso ok; altrimenti vuota)
    collections_map = get_collections_map()

    created = 0
    updated = 0
    errors = 0

    for (row, colmap) in read_csv(csv_path):
        nome = (row.get(colmap["nome_articolo"]) or "").strip()
        price = parse_price(row.get(colmap["prezzo_eur"]))
        sku = (row.get(colmap["sku"]) or "").strip()
        brand = (row.get(colmap["brand"]) or "").strip()
        categoria = (row.get(colmap["categoria"]) or "").strip()
        descr = (row.get(colmap["descrizione"]) or "").strip()
        preorder_deadline = (row.get(colmap["preorder_scadenza"]) or "").strip()
        eta = (row.get(colmap["eta"]) or "").strip()

        if not nome or not price or not sku:
            warn("Riga ignorata: nome/prezzo/sku mancanti.")
            continue

        log(f"[WORK] {nome[:70]} (SKU={sku})")

        descr_html = html_description(preorder_deadline, eta, descr)
        payload = build_product_payload(nome, sku, brand, price, descr_html)

        product = query_product_by_sku(sku)
        pid = None

        try:
            if product:
                pid = product.get("id")
                # Aggiorno base fields (descrizione, brand, visibile, opzioni)
                update_payload = {
                    "description": payload["description"],
                    "brand": payload.get("brand"),
                    "visible": True,
                    "manageVariants": True,
                    "productOptions": payload["productOptions"],
                    "priceData": payload["priceData"]
                }
                update_product(pid, update_payload)
                updated += 1
            else:
                pid = create_product(payload)
                created += 1

            # Varianti prezzo
            try:
                variants = build_variant_overrides(price, sku)
                patch_variants(pid, variants)
            except requests.HTTPError as e:
                # se chiede 'Product variants must be managed', riprovo forzando manageVariants
                if "Product variants must be managed" in str(e):
                    try:
                        update_product(pid, {"manageVariants": True, "productOptions": payload["productOptions"]})
                        patch_variants(pid, variants)
                    except Exception as e2:
                        warn(f"Varianti: secondo tentativo fallito: {e2}")
                else:
                    warn(f"Varianti: PATCH fallita: {e}")

            # Categoria
            if categoria and collections_map:
                coll_id = collections_map.get(norm(categoria))
                if coll_id:
                    add_to_collection(coll_id, pid)
                else:
                    warn(f"Categoria '{categoria}' non trovata nelle collections caricate.")

        except requests.HTTPError as e:
            err(f"Riga '{nome}': {e}")
            errors += 1
        except Exception as e:
            err(f"Riga '{nome}': {e}")
            errors += 1

    log(f"[DONE] Creati: {created}, Aggiornati: {updated}, Errori: {errors}")
    if errors:
        sys.exit(2)

if __name__ == "__main__":
    main()
