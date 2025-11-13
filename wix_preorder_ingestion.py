#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os, sys, csv, json, time
from typing import Dict, Any, Optional, Tuple, List
import requests
from html import escape

API_BASE = "https://www.wixapis.com"
DEPOSIT_PCT = 0.20  # 20% per ANTICIPO/SALDO. Cambia se vuoi 0.30 = 30%

# ====== HTTP ======
def headers() -> Dict[str, str]:
    api_key = os.environ.get("WIX_API_KEY")
    site_id = os.environ.get("WIX_SITE_ID")
    if not api_key or not site_id:
        raise RuntimeError("Variabili WIX_API_KEY e/o WIX_SITE_ID mancanti.")
    return {
        "Content-Type": "application/json",
        "Authorization": api_key.strip(),      # chiave raw (non Bearer)
        "wix-site-id": site_id.strip()
    }

def wreq(method: str, path: str, payload: Optional[Dict[str, Any]] = None) -> requests.Response:
    url = API_BASE + path
    h = headers()
    data = json.dumps(payload) if payload is not None else None
    r = requests.request(method=method, url=url, headers=h, data=data, timeout=30)
    # Log sintetico sugli errori
    if r.status_code >= 400:
        try:
            j = r.json()
            msg = json.dumps(j, ensure_ascii=False)
        except Exception:
            msg = r.text
        raise RuntimeError(f"{method} {path} failed {r.status_code}: {msg}")
    return r

# ====== Util ======
def read_csv(csv_path: str) -> List[Dict[str, str]]:
    # CSV del tuo template: delimitatore ';', encoding con BOM ok
    with open(csv_path, "r", encoding="utf-8-sig", newline="") as fh:
        reader = csv.DictReader(fh, delimiter=';')
        rows = [ {k.strip(): (v or "").strip() for k,v in row.items()} for row in reader ]
    # mappatura colonne (nomi del tuo XLS v7)
    needed = ["nome_articolo", "prezzo_eur", "sku", "brand", "categoria", "descrizione", "preorder_scadenza", "eta"]
    missing = [c for c in needed if c not in rows[0].keys()]
    if missing:
        raise RuntimeError(f"CSV mancano colonne: {missing}")
    return rows

def trunc_name(name: str) -> str:
    name = name.strip()
    return name[:80] if len(name) > 80 else name

def build_description(deadline: str, eta: str, descr: str) -> str:
    # HTML con riga vuota tra ETA e descrizione
    safe_descr = escape(descr).replace("\n", "<br>")
    parts = []
    if deadline:
        parts.append(f"<p><strong>Preorder Deadline:</strong> {escape(deadline)}</p>")
    if eta:
        parts.append(f"<p><strong>ETA:</strong> {escape(eta)}</p>")
    # riga vuota
    parts.append("<p>&nbsp;</p>")
    parts.append(f"<p>{safe_descr}</p>")
    return "".join(parts)

def parse_price(val: str) -> float:
    # supporta “1.234,56” o “1234.56”
    v = val.replace(".", "").replace(",", ".")
    return round(float(v), 2)

# ====== Wix helpers ======
def query_product_by_sku(sku: str) -> Optional[Dict[str, Any]]:
    # stores-reader v1 /products/query con filtro JSON, non stringa!
    body = {
        "filter": {"sku": sku},
        "limit": 1
    }
    try:
        r = wreq("POST", "/stores-reader/v1/products/query", body)
        data = r.json()
        items = data.get("items") or data.get("products") or []
        return items[0] if items else None
    except Exception as e:
        # Non blocco: se 404 su reader, ci penso dopo
        print(f"[WARN] Query SKU fallita {sku}: {e}")
        return None

def create_product_minimal(name: str, sku: str, brand: str, full_price: float, description_html: str) -> Dict[str, Any]:
    product = {
        "name": name,
        "productType": "physical",
        "sku": sku,
        "brand": brand,
        "visible": True,
        "manageVariants": True,
        "priceData": {"currency": "EUR", "price": full_price},
        "description": description_html
    }
    payload = {"product": product}
    r = wreq("POST", "/stores/v1/products", payload)
    return r.json()["product"]

def update_product_options(product_id: str) -> None:
    # Crea l’opzione con le 2 scelte
    product = {
        "id": product_id,
        "manageVariants": True,
        "productOptions": [
            {
                "name": "PREORDER PAYMENTS OPTIONS*",
                "type": "drop_down",
                "choices": [
                    {"description": "ANTICIPO/SALDO"},
                    {"description": "PAGAMENTO ANTICIPATO"}
                ]
            }
        ]
    }
    payload = {"product": product}
    wreq("PATCH", f"/stores/v1/products/{product_id}", payload)

def patch_variants_price(product_id: str, sku_base: str, full_price: float) -> None:
    # Wix v1 si aspetta choices come OGGETTO { "NomeOpzione": "DescrizioneScelta" }
    deposit = round(full_price * DEPOSIT_PCT, 2)
    variants = [
        {
            "choices": {"PREORDER PAYMENTS OPTIONS*": "ANTICIPO/SALDO"},
            "priceData": {"currency": "EUR", "price": deposit},
            "visible": True,
            "inStock": True,
            "sku": f"{sku_base}-ANT"
        },
        {
            "choices": {"PREORDER PAYMENTS OPTIONS*": "PAGAMENTO ANTICIPATO"},
            "priceData": {"currency": "EUR", "price": full_price},
            "visible": True,
            "inStock": True,
            "sku": f"{sku_base}-FULL"
        }
    ]
    payload = {"variants": variants}
    wreq("PATCH", f"/stores/v1/products/{product_id}/variants", payload)

def load_collections_map() -> Dict[str, str]:
    # Mappa titolo collection (lower) -> id
    try:
        r = wreq("GET", "/stores/v1/collections?limit=100")
        data = r.json()
        items = data.get("collections") or data.get("items") or []
        out = {}
        for c in items:
            title = (c.get("name") or c.get("title") or "").strip().lower()
            cid = c.get("id")
            if title and cid:
                out[title] = cid
        return out
    except Exception as e:
        print(f"[WARN] Lettura categorie fallita: {e}")
        return {}

def attach_to_collection(product_id: str, collections_map: Dict[str,str], categoria: str) -> None:
    if not categoria:
        return
    key = categoria.strip().lower()
    cid = collections_map.get(key)
    if not cid:
        # Provo senza accenti/extra spazi
        key2 = " ".join(key.split())
        cid = collections_map.get(key2)
    if not cid:
        print(f"[WARN] Categoria non trovata: {categoria}")
        return
    payload = {"productIds": [product_id]}
    wreq("POST", f"/stores/v1/collections/{cid}/products/add", payload)

# ====== Main ======
def process_row(rownum: int, row: Dict[str, str], collections_map: Dict[str,str]) -> Tuple[bool, str]:
    name_raw = row.get("nome_articolo", "")
    name = trunc_name(name_raw)
    sku = row.get("sku", "").strip()
    brand = row.get("brand", "").strip()
    categoria = row.get("categoria", "").strip()
    descr = row.get("descrizione", "")
    deadline = row.get("preorder_scadenza", "")
    eta = row.get("eta", "")
    price_str = row.get("prezzo_eur", "0")

    if not sku or not name:
        return False, f"[ERRORE] Riga {rownum} campi mancanti: sku/nome"

    try:
        full_price = parse_price(price_str)
    except Exception:
        return False, f"[ERRORE] Riga {rownum} prezzo non valido: {price_str}"

    description_html = build_description(deadline, eta, descr)

    # 1) Cerco per SKU
    existing = query_product_by_sku(sku)

    try:
        if existing:
            pid = existing.get("id")
            if not pid:
                # fallback: eseguo GET listing per sicurezza? Non necessario qui
                return False, f"[ERRORE] Riga {rownum} prodotto trovato ma senza id (SKU={sku})"

            # Aggiorno nome/brand/descrizione/prezzo e abilito varianti
            product = {
                "id": pid,
                "name": name,
                "brand": brand,
                "description": description_html,
                "manageVariants": True,
                "priceData": {"currency": "EUR", "price": full_price},
                # opzionale, se supportato dallo store:
                # "isPreOrder": True
            }
            wreq("PATCH", f"/stores/v1/products/{pid}", {"product": product})

            # Opzioni + varianti
            update_product_options(pid)
            patch_variants_price(pid, sku, full_price)

            # Categoria se disponibile
            attach_to_collection(pid, collections_map, categoria)

            return True, f"[OK] Aggiornato '{name}' (SKU={sku})"

        else:
            # Creo nuovo prodotto minimale
            created = create_product_minimal(name, sku, brand, full_price, description_html)
            pid = created["id"]

            # Opzioni + varianti
            update_product_options(pid)
            patch_variants_price(pid, sku, full_price)

            # Categoria
            attach_to_collection(pid, collections_map, categoria)

            return True, f"[NEW] Creato '{name}' (SKU={sku})"
    except Exception as e:
        return False, f"[ERRORE] Riga {rownum} '{name}': {e}"

def main():
    csv_path = sys.argv[1] if len(sys.argv) > 1 else os.environ.get("CSV_PATH") or "input/template_preordini_v7.csv"
    print(f"[INFO] CSV: {csv_path}")

    rows = read_csv(csv_path)
    collections_map = load_collections_map()

    ok = 0
    err = 0
    for i, row in enumerate(rows, start=2):  # numerazione come file (prima riga intestazioni)
        name_show = trunc_name(row.get("nome_articolo",""))
        sku = row.get("sku","")
        print(f"[WORK] {name_show} (SKU={sku})")
        success, msg = process_row(i, row, collections_map)
        print(msg)
        if success:
            ok += 1
        else:
            err += 1

    print(f"[DONE] Creati/Aggiornati: {ok}, Errori: {err}")
    sys.exit(0 if err == 0 else 2)

if __name__ == "__main__":
    main()
