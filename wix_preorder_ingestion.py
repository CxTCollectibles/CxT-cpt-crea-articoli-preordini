#!/usr/bin/env python3
import os
import sys
import csv
import json
import time
from decimal import Decimal, ROUND_HALF_UP
import requests

# ================== CONFIG ==================
CSV_DEFAULT = "input/template_preordini_v7.csv"

WIX_API_BASE = "https://www.wixapis.com"
WIX_API_KEY = os.environ.get("WIX_API_KEY")
WIX_SITE_ID = os.environ.get("WIX_SITE_ID")

# Varianti/prezzi
DEPOSIT_PCT = Decimal("0.30")        # 30% ANTICIPO/SALDO
EARLY_PAY_DISC = Decimal("0.05")     # 5% sconto PAGAMENTO ANTICIPATO
OPTION_TITLE = "PREORDER PAYMENTS OPTIONS*"
CHOICE_DEPOSIT = "ANTICIPO/SALDO"
CHOICE_EARLY = "PAGAMENTO ANTICIPATO"

# ================== HTTP ==================
def headers():
    if not WIX_API_KEY or not WIX_SITE_ID:
        raise RuntimeError("WIX_API_KEY e/o WIX_SITE_ID mancanti.")
    return {
        "Authorization": WIX_API_KEY,
        "wix-site-id": WIX_SITE_ID,
        "Content-Type": "application/json",
    }

def req(method: str, path: str, payload: dict | None = None):
    url = f"{WIX_API_BASE}{path}"
    data = json.dumps(payload) if payload is not None else None
    r = requests.request(method, url, headers=headers(), data=data, timeout=30)
    if r.status_code >= 400:
        raise RuntimeError(f"{method} {path} failed {r.status_code}: {r.text}")
    if not r.text.strip():
        return {}
    try:
        return r.json()
    except Exception:
        return {}

# ================== UTIL ==================
def money(val) -> float:
    d = Decimal(str(val)).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
    return float(d)

def norm_price(raw: str) -> Decimal:
    if raw is None:
        return Decimal("0")
    s = str(raw).strip().replace(",", ".")
    return Decimal(s) if s else Decimal("0")

def safe_name(name: str) -> str:
    name = (name or "").strip()
    if len(name) > 80:
        print("[WARN] Nome > 80 caratteri, troncato.")
        name = name[:80]
    return name

# ================== CSV ==================
COLS = [
    "nome_articolo", "prezzo_eur", "sku", "brand",
    "categoria", "descrizione", "preorder_scadenza", "eta"
]

def read_csv(path: str):
    with open(path, "r", encoding="utf-8-sig", newline="") as fh:
        rd = csv.DictReader(fh, delimiter=";")
        missing = [c for c in COLS if c not in rd.fieldnames]
        if missing:
            raise RuntimeError(f"CSV mancano colonne: {missing}")
        for i, row in enumerate(rd, start=2):
            yield i, row

# ================== PRODOTTI ==================
def query_product_by_sku(sku: str) -> dict | None:
    # Wix vuole il filtro come stringa (non oggetto)
    body = {"query": {"filter": f'sku eq "{sku}"', "paging": {"limit": 1}}}
    try:
        res = req("POST", "/stores-reader/v1/products/query", body)
        items = (res or {}).get("items") or []
        return items[0] if items else None
    except Exception as e:
        print(f"[WARN] Query SKU fallita {sku}: {e}")
        try:
            res = req("POST", "/stores/v1/products/query", body)
            items = (res or {}).get("items") or []
            return items[0] if items else None
        except Exception as e2:
            print(f"[WARN] Query SKU (fallback) fallita {sku}: {e2}")
            return None

def build_description(preorder_scadenza: str, eta: str, descr: str) -> str:
    parts = []
    if preorder_scadenza:
        parts.append(f"PREORDER DEADLINE: {preorder_scadenza}")
    if eta:
        parts.append(f"ETA: {eta}")
    if parts:
        parts.append("")  # riga vuota
    if descr:
        parts.append(descr)
    return "\n".join(parts)

def create_product(name: str, price: Decimal, sku: str, brand: str, description: str) -> str:
    descr_deposit = f"Paga {int(DEPOSIT_PCT*100)}% ora, saldo alla disponibilità"
    descr_early   = f"Pagamento immediato con sconto {int(EARLY_PAY_DISC*100)}%"

    body = {
        "product": {
            "productType": "physical",
            "name": name,
            "sku": str(sku),
            "brand": brand or "",
            "description": description or "",
            "priceData": {"price": money(price)},
            "visible": True,
            "manageVariants": True,
            "productOptions": [
                {
                    "name": OPTION_TITLE,
                    "choices": [
                        {"value": CHOICE_DEPOSIT, "description": descr_deposit},
                        {"value": CHOICE_EARLY,   "description": descr_early},
                    ],
                }
            ],
        }
    }
    res = req("POST", "/stores/v1/products", body)
    return (res.get("product") or {}).get("id")

def patch_product(product_id: str, name: str, price: Decimal, brand: str, description: str):
    body = {
        "product": {
            "name": name,
            "brand": brand or "",
            "description": description or "",
            "priceData": {"price": money(price)},
            "productType": "physical",
            "manageVariants": True,
        }
    }
    req("PATCH", f"/stores/v1/products/{product_id}", body)

# ================== VARIANTI ==================
def ensure_option(product_id: str) -> str:
    # Garantisce che esista OPTION_TITLE con scelte e descrizioni non vuote
    prod = req("GET", f"/stores/v1/products/{product_id}")
    product = prod.get("product", {}) if isinstance(prod, dict) else {}
    opts = product.get("productOptions") or []

    descr_deposit = f"Paga {int(DEPOSIT_PCT*100)}% ora, saldo alla disponibilità"
    descr_early   = f"Pagamento immediato con sconto {int(EARLY_PAY_DISC*100)}%"

    for opt in opts:
        choices = opt.get("choices") or []
        values_upper = [ (c or {}).get("value","").strip().upper() for c in choices ]
        if CHOICE_DEPOSIT in values_upper and CHOICE_EARLY in values_upper:
            changed = False
            for ch in choices:
                if ch.get("value") == CHOICE_DEPOSIT and not (ch.get("description") or "").strip():
                    ch["description"] = descr_deposit; changed = True
                if ch.get("value") == CHOICE_EARLY and not (ch.get("description") or "").strip():
                    ch["description"] = descr_early; changed = True
            if not product.get("manageVariants"):
                product["manageVariants"] = True; changed = True
            if changed:
                req("PATCH", f"/stores/v1/products/{product_id}", {"product": {"productOptions": opts, "manageVariants": True}})
                time.sleep(0.4)
            # Il PATCH variants richiede "title" = nome dell'opzione
            return opt.get("name") or opt.get("title") or OPTION_TITLE

    patch_body = {
        "product": {
            "manageVariants": True,
            "productOptions": [
                {
                    "name": OPTION_TITLE,
                    "choices": [
                        {"value": CHOICE_DEPOSIT, "description": descr_deposit},
                        {"value": CHOICE_EARLY,   "description": descr_early},
                    ],
                }
            ],
        }
    }
    req("PATCH", f"/stores/v1/products/{product_id}", patch_body)
    time.sleep(0.6)
    return OPTION_TITLE

def set_variant_prices(product_id: str, option_name: str, base_price: Decimal):
    price_deposit = (base_price * DEPOSIT_PCT).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
    price_early   = (base_price * (Decimal("1") - EARLY_PAY_DISC)).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)

    # ATTENZIONE: Wix si aspetta choices come LISTA di oggetti {title, description}
    body = {
        "variants": [
            {
                "choices": [{"title": option_name, "description": CHOICE_DEPOSIT}],
                "priceData": {"price": money(price_deposit)}
            },
            {
                "choices": [{"title": option_name, "description": CHOICE_EARLY}],
                "priceData": {"price": money(price_early)}
            },
        ]
    }
    req("PATCH", f"/stores/v1/products/{product_id}/variants", body)

def apply_preorder_variants(product_id: str, base_price: Decimal):
    opt_name = ensure_option(product_id)
    # piccolo ritardo per coerenza interna di Wix
    time.sleep(0.4)
    set_variant_prices(product_id, opt_name, base_price)

# ================== MAIN ==================
def main():
    csv_path = sys.argv[1] if len(sys.argv) > 1 else CSV_DEFAULT
    print(f"[INFO] CSV: {csv_path}")

    created = 0
    updated = 0
    errors = 0

    for rownum, row in read_csv(csv_path):
        name = safe_name(row.get("nome_articolo", ""))
        try:
            sku = str(row.get("sku", "")).strip()
            brand = (row.get("brand") or "").strip()
            prezzo = norm_price(row.get("prezzo_eur", "0"))
            descrizione = build_description(
                row.get("preorder_scadenza", "").strip(),
                row.get("eta", "").strip(),
                (row.get("descrizione") or "").strip()
            )

            print(f"[WORK] {name} (SKU={sku})")

            existing = query_product_by_sku(sku)
            if not existing:
                try:
                    product_id = create_product(name, prezzo, sku, brand, descrizione)
                    print(f"[NEW] Creato {sku} -> {product_id}")
                    created += 1
                except Exception as ce:
                    if "sku is not unique" in str(ce).lower():
                        existing = query_product_by_sku(sku)
                        if not existing:
                            raise
                        product_id = existing.get("id")
                        print(f"[INFO] SKU duplicato, uso esistente {product_id}")
                    else:
                        raise
            else:
                product_id = existing.get("id")
                patch_product(product_id, name, prezzo, brand, descrizione)
                print(f"[UPD] Aggiornato {sku} -> {product_id}")
                updated += 1

            apply_preorder_variants(product_id, prezzo)

        except Exception as e:
            print(f"[ERRORE] Riga {rownum} '{name}': {e}")
            errors += 1

    print(f"[DONE] Creati/Aggiornati: {created+updated}, Errori: {errors}")
    if errors:
        sys.exit(2)

if __name__ == "__main__":
    main()
