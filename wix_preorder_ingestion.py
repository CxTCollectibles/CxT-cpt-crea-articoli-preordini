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
WIX_API_KEY  = os.environ.get("WIX_API_KEY")
WIX_SITE_ID  = os.environ.get("WIX_SITE_ID")

DEPOSIT_PCT     = Decimal("0.30")   # ANTICIPO/SALDO = 30%
EARLY_PAY_DISC  = Decimal("0.05")   # PAGAMENTO ANTICIPATO = -5%
OPTION_TITLE    = "PREORDER PAYMENTS OPTIONS*"
CHOICE_DEPOSIT  = "ANTICIPO/SALDO"
CHOICE_EARLY    = "PAGAMENTO ANTICIPATO"

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
    if not (r.text or "").strip():
        return {}
    try:
        return r.json()
    except Exception:
        return {}

# ================== UTIL ==================
def money(d: Decimal | float | str) -> float:
    q = Decimal(str(d)).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
    return float(q)

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
REQUIRED_COLS = [
    "nome_articolo", "prezzo_eur", "sku", "brand",
    "categoria", "descrizione", "preorder_scadenza", "eta"
]

def read_csv(path: str):
    with open(path, "r", encoding="utf-8-sig", newline="") as fh:
        rd = csv.DictReader(fh, delimiter=";")
        missing = [c for c in REQUIRED_COLS if c not in rd.fieldnames]
        if missing:
            raise RuntimeError(f"CSV mancano colonne: {missing}")
        for i, row in enumerate(rd, start=2):
            yield i, row

# ================== LOOKUP PRODOTTI ==================
def query_page(limit=100, cursor=None):
    body = {"query": {"paging": {"limit": limit}}}
    if cursor:
        body["query"]["paging"]["cursor"] = cursor
    return req("POST", "/stores/v1/products/query", body)

def find_by_sku_scan(sku: str) -> dict | None:
    """Scansione paginata senza filtri (evita errori parser su filter)."""
    cursor = None
    for _ in range(50):  # guardrail
        res = query_page(limit=100, cursor=cursor)
        items = (res or {}).get("items") or []
        for it in items:
            if str(it.get("sku", "")).strip() == sku:
                return it
        cursor = (res.get("paging") or {}).get("nextCursor") or res.get("nextCursor")
        if not cursor or not items:
            break
    return None

# ================== DESCRIZIONE ==================
def build_description(preorder_scadenza: str, eta: str, descr: str) -> str:
    preorder_scadenza = (preorder_scadenza or "").strip()
    eta = (eta or "").strip()
    descr = (descr or "").strip()
    parts = []
    if preorder_scadenza:
        parts.append(f"PREORDER DEADLINE: {preorder_scadenza}")
    if eta:
        parts.append(f"ETA: {eta}")
    if parts:
        parts.append("")  # riga vuota fra header e corpo
    if descr:
        parts.append(descr)
    return "\n".join(parts)

# ================== CREATE / PATCH ==================
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
            "manageVariants": True
        }
    }
    req("PATCH", f"/stores/v1/products/{product_id}", body)

# ================== VARIANTI ==================
def get_product(product_id: str) -> dict:
    res = req("GET", f"/stores/v1/products/{product_id}")
    return res.get("product", {}) if isinstance(res, dict) else {}

def ensure_option(product_id: str) -> str:
    """Garantisce l'opzione e ritorna il nome esatto da usare come 'key' del dict choices."""
    descr_deposit = f"Paga {int(DEPOSIT_PCT*100)}% ora, saldo alla disponibilità"
    descr_early   = f"Pagamento immediato con sconto {int(EARLY_PAY_DISC*100)}%"

    for attempt in range(8):
        prod = get_product(product_id)
        opts = prod.get("productOptions") or []
        for opt in opts:
            values = [ (c or {}).get("value","").strip().upper() for c in (opt.get("choices") or []) ]
            if CHOICE_DEPOSIT.upper() in values and CHOICE_EARLY.upper() in values:
                changed = False
                for ch in (opt.get("choices") or []):
                    if ch.get("value","").strip().upper() == CHOICE_DEPOSIT.upper() and not (ch.get("description") or "").strip():
                        ch["description"] = descr_deposit; changed = True
                    if ch.get("value","").strip().upper() == CHOICE_EARLY.upper() and not (ch.get("description") or "").strip():
                        ch["description"] = descr_early; changed = True
                if not prod.get("manageVariants"):
                    changed = True
                if changed:
                    req("PATCH", f"/stores/v1/products/{product_id}", {
                        "product": {"productOptions": opts, "manageVariants": True}
                    })
                    time.sleep(0.5)
                return opt.get("name") or opt.get("title") or OPTION_TITLE

        # se non c'è, la creo ora
        req("PATCH", f"/stores/v1/products/{product_id}", {
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
        })
        time.sleep(0.8)  # tempo perché Wix generi le varianti

    return OPTION_TITLE

def set_variant_prices(product_id: str, option_name: str, base_price: Decimal):
    price_deposit = (base_price * DEPOSIT_PCT).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
    price_early   = (base_price * (Decimal("1") - EARLY_PAY_DISC)).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)

    # *** QUI il formato richiesto da Wix v1: choices = oggetto { "NomeOpzione": "ValoreScelta" }
    body = {
        "variants": [
            {
                "choices": { option_name: CHOICE_DEPOSIT },
                "priceData": { "price": money(price_deposit) }
            },
            {
                "choices": { option_name: CHOICE_EARLY },
                "priceData": { "price": money(price_early) }
            },
        ]
    }
    req("PATCH", f"/stores/v1/products/{product_id}/variants", body)

def apply_preorder_variants(product_id: str, base_price: Decimal):
    opt_name = ensure_option(product_id)
    time.sleep(0.5)
    set_variant_prices(product_id, opt_name, base_price)

# ================== MAIN ==================
def main():
    csv_path = sys.argv[1] if len(sys.argv) > 1 else CSV_DEFAULT
    print(f"[INFO] CSV: {csv_path}")

    done = 0
    errs = 0

    for rownum, row in read_csv(csv_path):
        name  = safe_name(row.get("nome_articolo", ""))
        sku   = (row.get("sku") or "").strip()
        brand = (row.get("brand") or "").strip()
        price = norm_price(row.get("prezzo_eur"))
        descr = build_description(row.get("preorder_scadenza"), row.get("eta"), row.get("descrizione"))

        try:
            print(f"[WORK] {name} (SKU={sku})")

            prod = find_by_sku_scan(sku)
            if not prod:
                try:
                    pid = create_product(name, price, sku, brand, descr)
                    print(f"[NEW] Creato {sku} -> {pid}")
                except Exception as e:
                    if "sku is not unique" in str(e).lower():
                        prod = find_by_sku_scan(sku)
                        if not prod:
                            raise
                        pid = prod.get("id")
                        print(f"[INFO] SKU già presente, uso {pid}")
                    else:
                        raise
            else:
                pid = prod.get("id")
                patch_product(pid, name, price, brand, descr)
                print(f"[UPD] Aggiornato {sku} -> {pid}")

            apply_preorder_variants(pid, price)
            done += 1

        except Exception as e:
            print(f"[ERRORE] Riga {rownum} '{name}': {e}")
            errs += 1

    print(f"[DONE] Creati/Aggiornati: {done}, Errori: {errs}")
    if errs:
        sys.exit(2)

if __name__ == "__main__":
    main()
