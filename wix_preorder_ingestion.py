#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import csv
import json
import os
import re
import sys
from typing import Dict, Any, List, Optional

import requests
from unidecode import unidecode

WIX_BASE = "https://www.wixapis.com"

API_KEY = os.getenv("WIX_API_KEY", "").strip()
SITE_ID = os.getenv("WIX_SITE_ID", "").strip()

CSV_DEFAULT = "input/template_preordini_v7.csv"

# ==========================
# Logging basilare
# ==========================
def log(msg: str): print(msg, flush=True)
def warn(msg: str): print(f"[WARN] {msg}", flush=True)
def err(msg: str): print(f"[ERRORE] {msg}", flush=True)

# ==========================
# HTTP helper
# ==========================
def headers() -> Dict[str, str]:
    if not API_KEY or not SITE_ID:
        raise RuntimeError("WIX_API_KEY e/o WIX_SITE_ID mancanti (secrets).")
    return {
        "Authorization": API_KEY,        # API key in chiaro (NO Bearer)
        "wix-site-id": SITE_ID,          # deve essere il metaSiteId
        "Content-Type": "application/json",
    }

def req(method: str, path: str, body: Optional[Dict[str, Any]] = None, ok=(200, 201)):
    url = f"{WIX_BASE}{path}"
    data = json.dumps(body) if body is not None else None
    r = requests.request(method, url, headers=headers(), data=data, timeout=30)
    if r.status_code not in ok:
        raise requests.HTTPError(f"{r.status_code} {r.text}".strip(), response=r)
    if r.text and "application/json" in r.headers.get("Content-Type", ""):
        return r.json()
    return None

# ==========================
# Utilità
# ==========================
def norm(s: str) -> str:
    return unidecode((s or "").strip().lower())

def slugify(name: str) -> str:
    s = unidecode(name).strip().lower()
    s = re.sub(r"[^a-z0-9\- ]+", "", s)
    s = re.sub(r"\s+", "-", s)
    return s[:80] if len(s) > 80 else s

def parse_price(v: str) -> float:
    if v is None: return 0.0
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
    pre_deadline = (pre_deadline or "").strip()
    eta = (eta or "").strip()
    descr = (descr or "").replace("\r\n", "\n").replace("\r", "\n").strip()

    parts = []
    if pre_deadline:
        parts.append(f"<p>Preorder Deadline: {pre_deadline}</p>")
    if eta:
        parts.append(f"<p>ETA: {eta}</p>")
    parts.append("<p>&nbsp;</p>")  # riga vuota
    if descr:
        parts.append(f"<p>{descr.replace('\n','<br>')}</p>")
    return "<div>" + "".join(parts) + "</div>"

# ==========================
# Collections (categorie)
# ==========================
def get_collections_map() -> Dict[str, str]:
    try:
        res = req("GET", "/stores/v1/collections?limit=100")
        items = (res or {}).get("collections", []) if isinstance(res, dict) else []
        if not items:
            warn("Nessuna collection caricata (o endpoint non disponibile).")
            return {}
        log("[INFO] Categorie caricate: " + ", ".join(norm(c.get("name","")) for c in items))
        return {norm(c.get("name","")): c.get("id") for c in items if c.get("id")}
    except requests.HTTPError as e:
        warn(f"Lettura categorie fallita: {e.response.status_code}")
        return {}

def add_to_collection(collection_id: str, product_id: str):
    try:
        body = {"productIds": [product_id]}
        req("POST", f"/stores/v1/collections/{collection_id}/products/add", body, ok=(200,201,204))
    except requests.HTTPError as e:
        warn(f"Add to collection fallita: {e}")

# ==========================
# Products v1
# ==========================
def query_product_by_sku(sku: str) -> Optional[Dict[str, Any]]:
    # Tentativo 1: query con filtro oggetto (NON stringa)
    try:
        body = {"query": {"filter": {"sku": sku}, "paging": {"limit": 50}}}
        res = req("POST", "/stores/v1/products/query", body)
        items = (res or {}).get("products", [])
        if items:
            return items[0]
    except requests.HTTPError as e:
        warn(f"Query SKU fallita {sku}: {e}")

    # Tentativo 2: best-effort scan (se endpoint c'è)
    try:
        cursor = None
        for _ in range(5):
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
    except requests.HTTPError as e:
        warn(f"Scan prodotti fallita: {e}")
    return None

def create_product(product_payload: Dict[str, Any]) -> str:
    res = req("POST", "/stores/v1/products", {"product": product_payload}, ok=(200,201))
    pid = (res or {}).get("product", {}).get("id") or (res or {}).get("id")
    if not pid:
        raise RuntimeError("ID prodotto non ricevuto.")
    return pid

def update_product(pid: str, product_payload: Dict[str, Any]):
    req("PATCH", f"/stores/v1/products/{pid}", {"product": product_payload}, ok=(200,201))

def patch_variants_choices_map(pid: str, variants: List[Dict[str, Any]]):
    # Scelta formato A: choices = { "OPTION_TITLE": "CHOICE" }
    body = {"variants": variants}
    req("PATCH", f"/stores/v1/products/{pid}/variants", body, ok=(200,201))

def patch_variants_choices_list(pid: str, variants_list_format: List[Dict[str, Any]]):
    # Scelta formato B: choices = [ { "optionTitle": "...", "choice": "..." } ]
    body = {"variants": variants_list_format}
    req("PATCH", f"/stores/v1/products/{pid}/variants", body, ok=(200,201))

# ==========================
# CSV
# ==========================
def _find_col(cols: List[str], candidates: List[str]) -> Optional[str]:
    nl = {norm(c): c for c in cols}
    for c in candidates:
        if norm(c) in nl:
            return nl[norm(c)]
    return None

def read_csv(path: str):
    with open(path, "r", encoding="utf-8-sig", newline="") as fh:
        reader = csv.DictReader(fh, delimiter=';')
        cols = [c.strip() for c in (reader.fieldnames or [])]

        colmap = {
            "nome_articolo": _find_col(cols, ["nome_articolo","titolo","name"]),
            "prezzo_eur": _find_col(cols, ["prezzo_eur","prezzo","price"]),
            "sku": _find_col(cols, ["sku"]),
            "brand": _find_col(cols, ["brand","marca"]),
            "categoria": _find_col(cols, ["categoria","collection","collezione"]),
            "descrizione": _find_col(cols, ["descrizione","description"]),
            "preorder_scadenza": _find_col(cols, ["preorder_scadenza","preorder_deadline","deadline"]),
            "eta": _find_col(cols, ["eta","arrivo","release"]),
        }
        log("[INFO] Mappatura colonne:")
        for k,v in colmap.items():
            log(f"  - {k}: {v}")

        # Minimi indispensabili
        for k in ("nome_articolo","prezzo_eur","sku"):
            if not colmap[k]:
                raise RuntimeError(f"CSV mancano colonne: ['{k}']")

        for row in reader:
            yield row, colmap

# ==========================
# Regole business
# ==========================
OPTION_TITLE = "PREORDER PAYMENTS OPTIONS*"
CHOICE_A = "ANTICIPO/SALDO"
CHOICE_P = "PAGAMENTO ANTICIPATO"

def build_product_payload(nome: str, sku: str, brand: str, price: float, descr_html: str) -> Dict[str, Any]:
    name80 = trunc_name(nome)
    payload = {
        "name": name80,
        "slug": slugify(name80),
        "sku": sku,
        "productType": "physical",   # fondamentale: senza wrapper 'product' non veniva letto
        "visible": True,
        "priceData": {"price": price, "currency": "EUR"},
        "description": descr_html,
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
    if brand:
        payload["brand"] = {"name": brand}
    return payload

def build_variants_for_map(price: float, sku: str) -> List[Dict[str, Any]]:
    # Formato A: choices = { OPTION_TITLE: CHOICE }
    return [
        {
            "choices": {OPTION_TITLE: CHOICE_A},
            "priceData": {"price": round(price * 0.30, 2), "currency": "EUR"},
            "sku": f"{sku}-A"
        },
        {
            "choices": {OPTION_TITLE: CHOICE_P},
            "priceData": {"price": round(price * 0.95, 2), "currency": "EUR"},
            "sku": f"{sku}-P"
        }
    ]

def build_variants_for_list(price: float, sku: str) -> List[Dict[str, Any]]:
    # Formato B: choices = [ {"optionTitle": "...", "choice": "..."} ]
    return [
        {
            "choices": [{"optionTitle": OPTION_TITLE, "choice": CHOICE_A}],
            "priceData": {"price": round(price * 0.30, 2), "currency": "EUR"},
            "sku": f"{sku}-A"
        },
        {
            "choices": [{"optionTitle": OPTION_TITLE, "choice": CHOICE_P}],
            "priceData": {"price": round(price * 0.95, 2), "currency": "EUR"},
            "sku": f"{sku}-P"
        }
    ]

# ==========================
# Main
# ==========================
def main():
    csv_path = sys.argv[1].strip() if len(sys.argv) > 1 else CSV_DEFAULT
    log(f"[INFO] CSV: {csv_path}")

    collections_map = get_collections_map()

    created = 0
    updated = 0
    errors = 0

    for (row, colmap) in read_csv(csv_path):
        nome = (row.get(colmap["nome_articolo"]) or "").strip()
        prezzo = parse_price(row.get(colmap["prezzo_eur"]))
        sku = (row.get(colmap["sku"]) or "").strip()
        brand = (row.get(colmap["brand"]) or "").strip()
        categoria = (row.get(colmap["categoria"]) or "").strip()
        descr = (row.get(colmap["descrizione"]) or "").strip()
        pre_deadline = (row.get(colmap["preorder_scadenza"]) or "").strip()
        eta = (row.get(colmap["eta"]) or "").strip()

        if not nome or not sku or not prezzo:
            warn("Riga ignorata: nome/prezzo/sku mancanti.")
            continue

        log(f"[WORK] {trunc_name(nome)} (SKU={sku})")
        descr_html = html_description(pre_deadline, eta, descr)
        product_payload = build_product_payload(nome, sku, brand, prezzo, descr_html)

        try:
            existing = query_product_by_sku(sku)
        except Exception as e:
            warn(f"Query SKU problema non bloccante: {e}")
            existing = None

        try:
            if existing:
                pid = existing.get("id")
                update_product(pid, {
                    "description": product_payload["description"],
                    "brand": product_payload.get("brand"),
                    "visible": True,
                    "manageVariants": True,
                    "productOptions": product_payload["productOptions"],
                    "priceData": product_payload["priceData"],
                })
                updated += 1
            else:
                pid = create_product(product_payload)
                created += 1

            # Varianti prezzo (doppio tentativo formato)
            try:
                patch_variants_choices_map(pid, build_variants_for_map(prezzo, sku))
            except requests.HTTPError as e1:
                warn(f"Varianti (map) fallite, provo formato list: {e1}")
                try:
                    patch_variants_choices_list(pid, build_variants_for_list(prezzo, sku))
                except Exception as e2:
                    warn(f"Varianti (list) ancora fallite: {e2}")

            # Categoria (se disponibile)
            if categoria and collections_map:
                coll_id = collections_map.get(norm(categoria))
                if coll_id:
                    add_to_collection(coll_id, pid)
                else:
                    warn(f"Categoria '{categoria}' non trovata tra le collections caricate.")

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
