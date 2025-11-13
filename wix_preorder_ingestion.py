#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os, sys, csv, json
from typing import Dict, Any, Optional, List
import requests
from html import escape

API_BASE = "https://www.wixapis.com"
DEPOSIT_PCT = 0.20  # 20% per ANTICIPO/SALDO. Cambia a 0.30 se vuoi il 30%

# ================= HTTP =================
def headers() -> Dict[str, str]:
    api_key = os.environ.get("WIX_API_KEY")
    site_id = os.environ.get("WIX_SITE_ID")
    if not api_key or not site_id:
        raise RuntimeError("Variabili WIX_API_KEY e/o WIX_SITE_ID mancanti.")
    return {
        "Content-Type": "application/json",
        "Authorization": api_key.strip(),   # chiave raw (non 'Bearer ')
        "wix-site-id": site_id.strip()
    }

def wreq(method: str, path: str, payload: Optional[Dict[str, Any]] = None) -> requests.Response:
    url = API_BASE + path
    data = json.dumps(payload) if payload is not None else None
    r = requests.request(method=method, url=url, headers=headers(), data=data, timeout=30)
    if r.status_code >= 400:
        # prova a estrarre JSON di errore, altrimenti testo
        try:
            msg = json.dumps(r.json(), ensure_ascii=False)
        except Exception:
            msg = r.text
        raise RuntimeError(f"{method} {path} failed {r.status_code}: {msg}")
    return r

# ================= UTIL =================
def read_csv(csv_path: str) -> List[Dict[str, str]]:
    with open(csv_path, "r", encoding="utf-8-sig", newline="") as fh:
        reader = csv.DictReader(fh, delimiter=';')
        rows = [{k.strip(): (v or "").strip() for k, v in row.items()} for row in reader]
    needed = ["nome_articolo", "prezzo_eur", "sku", "brand", "categoria", "descrizione", "preorder_scadenza", "eta"]
    if not rows:
        raise RuntimeError("CSV vuoto.")
    missing = [c for c in needed if c not in rows[0].keys()]
    if missing:
        raise RuntimeError(f"CSV mancano colonne: {missing}")
    return rows

def trunc_name(name: str) -> str:
    name = name.strip()
    return name[:80] if len(name) > 80 else name

def parse_price(val: str) -> float:
    # supporta "1.234,56" o "1234.56"
    v = val.replace(".", "").replace(",", ".")
    return round(float(v), 2)

def build_description(deadline: str, eta: str, descr: str) -> str:
    safe_descr = escape(descr).replace("\n", "<br>")
    parts = []
    if deadline:
        parts.append(f"<p><strong>Preorder Deadline:</strong> {escape(deadline)}</p>")
    if eta:
        parts.append(f"<p><strong>ETA:</strong> {escape(eta)}</p>")
    # riga vuota per separare
    parts.append("<p>&nbsp;</p>")
    parts.append(f"<p>{safe_descr}</p>")
    return "".join(parts)

# ========== WIX HELPERS ==========
def query_product_by_sku(sku: str) -> Optional[Dict[str, Any]]:
    # 1) Tentativo con corpo 'query.filter' e $eq
    bodies = [
        {"query": {"filter": {"sku": {"$eq": sku}}}, "limit": 1},
        {"filter": {"sku": {"$eq": sku}}, "limit": 1},
    ]
    for body in bodies:
        try:
            r = wreq("POST", "/stores-reader/v1/products/query", body)
            data = r.json()
            items = (
                data.get("items")
                or data.get("products")
                or data.get("itemsPage", {}).get("items")
                or []
            )
            # prendo solo se lo SKU corrisponde davvero
            for it in items:
                if (it.get("sku") or "").strip() == sku:
                    return it
        except Exception as e:
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
        "description": description_html,
        # Se supportato nel tuo tenant:
        # "isPreOrder": True
    }
    payload = {"product": product}
    r = wreq("POST", "/stores/v1/products", payload)
    return r.json()["product"]

def update_core_product(pid: str, name: str, brand: str, full_price: float, description_html: str) -> None:
    product = {
        "id": pid,
        "name": name,
        "brand": brand,
        "description": description_html,
        "manageVariants": True,
        "priceData": {"currency": "EUR", "price": full_price},
        # "isPreOrder": True
    }
    wreq("PATCH", f"/stores/v1/products/{pid}", {"product": product})

def ensure_payment_option(pid: str) -> None:
    # productOptions con 'value' NON vuoto
    option_name = "PREORDER PAYMENTS OPTIONS*"
    product = {
        "id": pid,
        "manageVariants": True,
        "productOptions": [
            {
                "name": option_name,
                "title": option_name,  # ridondante ma innocuo
                "type": "drop_down",
                "choices": [
                    {"value": "ANTICIPO/SALDO", "description": "ANTICIPO/SALDO"},
                    {"value": "PAGAMENTO ANTICIPATO", "description": "PAGAMENTO ANTICIPATO"}
                ]
            }
        ]
    }
    wreq("PATCH", f"/stores/v1/products/{pid}", {"product": product})

def upsert_variants(pid: str, sku_base: str, full_price: float) -> None:
    deposit = round(full_price * DEPOSIT_PCT, 2)
    # choices deve essere un oggetto: { "<NomeOpzione>": "<Value>" }
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
    wreq("PATCH", f"/stores/v1/products/{pid}/variants", {"variants": variants})

# ============== MAIN ==============
def process_row(rownum: int, row: Dict[str, str]) -> str:
    name_raw = row.get("nome_articolo", "")
    name = trunc_name(name_raw)
    sku = row.get("sku", "").strip()
    brand = row.get("brand", "").strip()
    descr = row.get("descrizione", "")
    deadline = row.get("preorder_scadenza", "")
    eta = row.get("eta", "")
    price_str = row.get("prezzo_eur", "0")

    if not sku or not name:
        return f"[ERRORE] Riga {rownum} campi mancanti: sku/nome"

    try:
        full_price = parse_price(price_str)
    except Exception:
        return f"[ERRORE] Riga {rownum} prezzo non valido: {price_str}"

    description_html = build_description(deadline, eta, descr)

    # Cerca esistenza via SKU
    existing = query_product_by_sku(sku)

    try:
        if existing and existing.get("id"):
            pid = existing["id"]
            update_core_product(pid, name, brand, full_price, description_html)
            ensure_payment_option(pid)
            upsert_variants(pid, sku, full_price)
            return f"[OK] Aggiornato '{name}' (SKU={sku})"
        else:
            created = create_product_minimal(name, sku, brand, full_price, description_html)
            pid = created["id"]
            ensure_payment_option(pid)
            upsert_variants(pid, sku, full_price)
            return f"[NEW] Creato '{name}' (SKU={sku})"
    except Exception as e:
        return f"[ERRORE] Riga {rownum} '{name}': {e}"

def main():
    csv_path = sys.argv[1] if len(sys.argv) > 1 else os.environ.get("CSV_PATH") or "input/template_preordini_v7.csv"
    print(f"[INFO] CSV: {csv_path}")

    rows = read_csv(csv_path)

    ok = 0
    err = 0
    for i, row in enumerate(rows, start=2):
        name_show = trunc_name(row.get("nome_articolo",""))
        sku = row.get("sku","")
        print(f"[WORK] {name_show} (SKU={sku})")
        msg = process_row(i, row)
        print(msg)
        if msg.startswith("[OK]") or msg.startswith("[NEW]"):
            ok += 1
        else:
            err += 1

    print(f"[DONE] Creati/Aggiornati: {ok}, Errori: {err}")
    sys.exit(0 if err == 0 else 2)

if __name__ == "__main__":
    main()
