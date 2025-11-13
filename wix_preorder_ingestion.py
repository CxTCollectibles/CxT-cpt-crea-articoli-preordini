#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os, sys, csv, json, math
from typing import Dict, Any, Optional, List
import requests
from html import escape

API_BASE = "https://www.wixapis.com"
DEPOSIT_PCT = 0.20  # 20% per ANTICIPO/SALDO

# ================= HTTP =================
def headers() -> Dict[str, str]:
    api_key = os.environ.get("WIX_API_KEY")
    site_id = os.environ.get("WIX_SITE_ID")
    if not api_key or not site_id:
        raise RuntimeError("Variabili WIX_API_KEY e/o WIX_SITE_ID mancanti.")
    return {
        "Content-Type": "application/json",
        "Authorization": api_key.strip(),   # raw key (non 'Bearer ')
        "wix-site-id": site_id.strip()
    }

def wreq(method: str, path: str, payload: Optional[Dict[str, Any]] = None) -> requests.Response:
    url = API_BASE + path
    data = json.dumps(payload) if payload is not None else None
    r = requests.request(method=method, url=url, headers=headers(), data=data, timeout=30)
    if r.status_code >= 400:
        try:
            msg = json.dumps(r.json(), ensure_ascii=False)
        except Exception:
            msg = r.text
        raise RuntimeError(f"{method} {path} failed {r.status_code}: {msg}")
    return r

# ================= CSV =================
def read_csv(csv_path: str) -> List[Dict[str, str]]:
    with open(csv_path, "r", encoding="utf-8-sig", newline="") as fh:
        reader = csv.DictReader(fh, delimiter=';')
        rows = [{(k or "").strip(): (v or "").strip() for k, v in row.items()} for row in reader]
    needed = ["nome_articolo", "prezzo_eur", "sku", "brand", "categoria", "descrizione", "preorder_scadenza", "eta"]
    if not rows:
        raise RuntimeError("CSV vuoto.")
    missing = [c for c in needed if c not in rows[0].keys()]
    if missing:
        raise RuntimeError(f"CSV mancano colonne: {missing}")
    return rows

def trunc_name(name: str) -> str:
    name = (name or "").strip()
    return name[:80] if len(name) > 80 else name

def parse_price(val: str) -> float:
    # supporta "1.234,56" o "1234.56"
    v = (val or "").replace(".", "").replace(",", ".")
    return round(float(v), 2)

def build_description(deadline: str, eta: str, descr: str) -> str:
    safe_descr = escape(descr or "").replace("\n", "<br>")
    parts = []
    if deadline:
        parts.append(f"<p><strong>Preorder Deadline:</strong> {escape(deadline)}</p>")
    if eta:
        parts.append(f"<p><strong>ETA:</strong> {escape(eta)}</p>")
    parts.append("<p>&nbsp;</p>")  # riga vuota
    parts.append(f"<p>{safe_descr}</p>")
    return "".join(parts)

# ========== WIX HELPERS ==========
def scan_products_find_by_sku(sku: str, max_pages: int = 30) -> Optional[Dict[str, Any]]:
    # Scansione paginata lato v1 (fallback robusto)
    cursor_keys = ["nextCursor", "nextPageCursor", "cursor", "pagingCursor"]
    cursor = None
    for page in range(1, max_pages+1):
        path = "/stores/v1/products?limit=100"
        if cursor:
            path += f"&cursor={cursor}"
        try:
            r = wreq("GET", path)
        except Exception as e:
            print(f"[WARN] Scan prodotti pagina {page} fallita: {e}")
            return None
        data = r.json() if r.text else {}
        items = data.get("products") or data.get("items") or []
        for it in items:
            if (it.get("sku") or "").strip() == sku:
                return it
        new_cursor = None
        for k in cursor_keys:
            if data.get(k):
                new_cursor = data[k]
                break
            if isinstance(data.get("paging"), dict) and data["paging"].get(k):
                new_cursor = data["paging"][k]
                break
        if not new_cursor:
            break
        cursor = new_cursor
    return None

def product_options_block() -> Dict[str, Any]:
    option_name = "PREORDER PAYMENTS OPTIONS*"
    return {
        "name": option_name,
        "title": option_name,
        "type": "drop_down",
        "choices": [
            {"value": "ANTICIPO/SALDO", "description": "ANTICIPO/SALDO"},
            {"value": "PAGAMENTO ANTICIPATO", "description": "PAGAMENTO ANTICIPATO"}
        ]
    }

def create_product_with_options(name: str, sku: str, brand: str, full_price: float, description_html: str) -> Dict[str, Any]:
    product: Dict[str, Any] = {
        "name": name,
        "productType": "physical",
        "sku": sku,
        "visible": True,
        "manageVariants": True,
        "priceData": {"currency": "EUR", "price": full_price},
        "description": description_html,
        "productOptions": [product_options_block()],
        # "isPreOrder": True,  # abilita se supportato sul tenant
    }
    if brand:
        product["brand"] = brand
    payload = {"product": product}
    r = wreq("POST", "/stores/v1/products", payload)
    return r.json().get("product") or r.json()

def update_product_core(pid: str, name: str, brand: str, full_price: float, description_html: str) -> None:
    product: Dict[str, Any] = {
        "id": pid,
        "name": name,
        "manageVariants": True,
        "priceData": {"currency": "EUR", "price": full_price},
        "description": description_html,
    }
    if brand:
        product["brand"] = brand
    wreq("PATCH", f"/stores/v1/products/{pid}", {"product": product})

def ensure_options(pid: str) -> None:
    # sovrascrivo l'opzione per sicurezza
    product = {
        "id": pid,
        "manageVariants": True,
        "productOptions": [product_options_block()]
    }
    wreq("PATCH", f"/stores/v1/products/{pid}", {"product": product})

def set_variants(pid: str, sku_base: str, full_price: float) -> None:
    deposit = round(full_price * DEPOSIT_PCT + 1e-9, 2)
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
    brand = (row.get("brand", "") or "").strip()
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

    # 1) Tenta creazione diretta (con opzioni già presenti)
    try:
        created = create_product_with_options(name, sku, brand, full_price, description_html)
        pid = created.get("id") or created.get("product", {}).get("id")
        if not pid:
            return f"[ERRORE] Riga {rownum} '{name}': risposta creazione senza id."
        set_variants(pid, sku, full_price)
        return f"[NEW] Creato '{name}' (SKU={sku})"
    except RuntimeError as e:
        msg = str(e)
        # SKU duplicato: individua ID prodotto via scansione e fai update
        if "sku is not unique" in msg or "already exists" in msg or "sku non unico" in msg:
            existing = scan_products_find_by_sku(sku)
            if not existing or not existing.get("id"):
                return f"[ERRORE] Riga {rownum} '{name}': SKU duplicato ma prodotto non trovato: {sku}"
            pid = existing["id"]
            try:
                update_product_core(pid, name, brand, full_price, description_html)
                ensure_options(pid)
                set_variants(pid, sku, full_price)
                return f"[OK] Aggiornato '{name}' (SKU={sku})"
            except Exception as e2:
                return f"[ERRORE] Riga {rownum} '{name}': {e2}"
        # manageVariants senza opzioni: riprova creando senza manageVariants, poi aggiunge opzioni e varianti
        if "manageVariants can't be true" in msg:
            try:
                # crea base senza manageVariants né options
                product_base = {
                    "name": name,
                    "productType": "physical",
                    "sku": sku,
                    "visible": True,
                    "manageVariants": False,
                    "priceData": {"currency": "EUR", "price": full_price},
                    "description": description_html,
                }
                if brand:
                    product_base["brand"] = brand
                created2 = wreq("POST", "/stores/v1/products", {"product": product_base}).json().get("product")
                pid2 = created2.get("id")
                ensure_options(pid2)
                set_variants(pid2, sku, full_price)
                return f"[NEW] Creato '{name}' (SKU={sku})"
            except Exception as e3:
                return f"[ERRORE] Riga {rownum} '{name}': {e3}"
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
