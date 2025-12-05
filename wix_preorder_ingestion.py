#!/usr/bin/env python3
import os
import csv
import json
import sys
import time
from typing import Dict, Any, Tuple
import requests

BASE = "https://www.wixapis.com"

WIX_API_KEY = os.environ.get("WIX_API_KEY", "").strip()
WIX_SITE_ID = os.environ.get("WIX_SITE_ID", "").strip()
CSV_PATH = sys.argv[1] if len(sys.argv) > 1 else os.environ.get("CSV_PATH", "input/template_preordini_v7.csv")

# Percentuale anticipo: default 30% (configurabile via env)
def _pct_env(val: str, default: float) -> float:
    try:
        return float(os.environ.get(val, str(default)))
    except Exception:
        return default

DEPOSIT_PCT = _pct_env("DEPOSIT_PCT", 0.30)

def headers() -> Dict[str, str]:
    if not WIX_API_KEY or not WIX_SITE_ID:
        print("[FATAL] Variabili WIX_API_KEY o WIX_SITE_ID mancanti.", file=sys.stderr)
        sys.exit(1)
    return {
        "Authorization": f"Bearer {WIX_API_KEY}",
        "wix-site-id": WIX_SITE_ID,
        "Content-Type": "application/json"
    }

def req(method: str, path: str, payload: Dict[str, Any] = None, ok=(200,201)) -> Tuple[int, Dict[str, Any]]:
    url = f"{BASE}{path}"
    data = json.dumps(payload) if payload is not None else None
    r = requests.request(method, url, headers=headers(), data=data, timeout=30)
    if r.status_code not in ok:
        body = r.text
        raise RuntimeError(f"{method} {path} failed {r.status_code}: {body}")
    if not r.text.strip():
        return r.status_code, {}
    try:
        return r.status_code, r.json()
    except Exception:
        return r.status_code, {}

def eur(v: float) -> float:
    return round(float(v) + 1e-9, 2)

def build_description(preorder_deadline: str, eta: str, descr_it: str) -> str:
    pd = (preorder_deadline or "").strip()
    et = (eta or "").strip()
    di = (descr_it or "").strip()
    di_html = di.replace("\n", "<br>")
    parts = []
    if pd:
        parts.append(f"<p><strong>Preorder Deadline:</strong> {pd}</p>")
    if et:
        parts.append(f"<p><strong>ETA:</strong> {et}</p>")
    parts.append("<p>&nbsp;</p>")  # riga vuota di separazione
    if di_html:
        parts.append(f"<p>{di_html}</p>")
    return "\n".join(parts)

def create_product(row: Dict[str, str]) -> str:
    nome = (row.get("nome_articolo") or "").strip()
    prezzo_str = (row.get("prezzo_eur") or "0").replace(",", ".")
    sku = (row.get("sku") or "").strip()
    brand = (row.get("brand") or "").strip()
    descr = (row.get("descrizione") or "").strip()
    preorder_scadenza = (row.get("preorder_scadenza") or row.get("preorder_deadline") or "").strip()
    eta = (row.get("eta") or "").strip()

    if not sku:
        raise RuntimeError("SKU mancante")

    try:
        prezzo = float(prezzo_str)
    except Exception:
        prezzo = 0.0

    descr_html = build_description(preorder_scadenza, eta, descr)

    product: Dict[str, Any] = {
        "name": nome[:80] if nome else sku,
        "productType": "physical",   # enum accetta "physical"
        "sku": sku,
        "priceData": {"currency": "EUR", "price": eur(prezzo)},
        "description": descr_html,
        "visible": True
    }
    if brand:
        # IMPORTANTISSIMO: brand deve essere STRINGA in v1, non oggetto
        product["brand"] = brand

    body = {"product": product}
    _status, js = req("POST", "/stores/v1/products", body, ok=(200,201))
    pid = js.get("product", {}).get("id")
    if not pid:
        raise RuntimeError(f"Creazione prodotto senza id. Risposta: {js}")
    return pid

def patch_add_option(product_id: str):
    option_name = "PREORDER PAYMENTS OPTIONS*"
    body = {
        "product": {
            "manageVariants": True,
            "productOptions": [
                {
                    "name": option_name,
                    "type": "drop_down",
                    "choices": [
                        {"value": "AS", "description": "ANTICIPO/SALDO"},
                        {"value": "PA", "description": "PAGAMENTO ANTICIPATO"}
                    ]
                }
            ]
        }
    }
    # Imposto l'opzione prima, in una PATCH dedicata
    req("PATCH", f"/stores/v1/products/{product_id}", body, ok=(200,))

def patch_add_variants(product_id: str, sku_base: str, full_price: float):
    option_name = "PREORDER PAYMENTS OPTIONS*"
    price_deposit = eur(full_price * DEPOSIT_PCT)
    price_full = eur(full_price)

    # Le varianti referenziano l’OPZIONE tramite il suo "name" e la SCELTA tramite la "description"
    body = {
        "product": {
            "variants": [
                {
                    "choices": { option_name: "ANTICIPO/SALDO" },
                    "priceData": {"currency": "EUR", "price": price_deposit},
                    "visible": True,
                    "sku": f"{sku_base}-AS"
                },
                {
                    "choices": { option_name: "PAGAMENTO ANTICIPATO" },
                    "priceData": {"currency": "EUR", "price": price_full},
                    "visible": True,
                    "sku": f"{sku_base}-PA"
                }
            ]
        }
    }
    req("PATCH", f"/stores/v1/products/{product_id}", body, ok=(200,))

def load_csv(path: str):
    with open(path, "r", encoding="utf-8-sig", newline="") as fh:
        reader = csv.DictReader(fh, delimiter=";")
        # Minimo indispensabile del tuo XLS V7
        expected = ["nome_articolo","prezzo_eur","sku","brand","descrizione","preorder_scadenza","eta"]
        missing = [c for c in expected if c not in reader.fieldnames]
        if missing:
            print(f"[WARN] CSV colonne mancanti: {missing}. Procedo comunque.")
        for row in reader:
            yield row

def main():
    print(f"[INFO] CSV: {CSV_PATH}")
    created = 0
    errors = 0

    for row in load_csv(CSV_PATH):
        nome = (row.get("nome_articolo") or "").strip()
        sku = (row.get("sku") or "").strip()
        prezzo_str = (row.get("prezzo_eur") or "0").replace(",", ".")
        try:
            prezzo = float(prezzo_str)
        except Exception:
            prezzo = 0.0

        display = (nome[:80] if nome else sku)
        print(f"[WORK] {display} (SKU={sku})")

        try:
            pid = create_product(row)
            print(f"[NEW] Creato {sku} -> {pid}")

            # Passo 1: opzioni
            try:
                patch_add_option(pid)
            except Exception as e:
                errors += 1
                print(f"[ERRORE] Opzioni {display}: {e}")
                continue  # senza opzioni non ha senso aggiungere varianti

            # Leggera attesa, Wix a volte è... lunatico
            time.sleep(0.3)

            # Passo 2: varianti con prezzi
            try:
                patch_add_variants(pid, sku, prezzo)
            except Exception as e:
                errors += 1
                print(f"[ERRORE] Varianti {display}: {e}")

            created += 1
            time.sleep(0.2)

        except Exception as e:
            errors += 1
            print(f"[ERRORE] Riga '{display}': {e}")

    print(f"[DONE] Creati/Aggiornati (base): {created}, Errori: {errors}")
    if errors:
        sys.exit(2)

if __name__ == "__main__":
    main()
