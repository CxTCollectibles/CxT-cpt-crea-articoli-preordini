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

def headers() -> Dict[str, str]:
    if not WIX_API_KEY or not WIX_SITE_ID:
        print("[FATAL] Variabili WIX_API_KEY o WIX_SITE_ID mancanti.", file=sys.stderr)
        sys.exit(1)
    return {
        "Authorization": f"Bearer {WIX_API_KEY}",  # formati che ti funzionavano
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
        parts.append(f"<p><strong>Preorder Deadline:</strong> {pd} <em>Salvo esaurimento</em></p>")
        parts.append("<p>&nbsp;</p>")  # riga vuota subito dopo la deadline
    if et:
        parts.append(f"<p><strong>ETA:</strong> {et}</p>")
    parts.append("<p>&nbsp;</p>")  # riga vuota di separazione dalla descrizione
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
        "name": (nome[:80] if nome else sku),
        "productType": "physical",   # enum accetta "physical"
        "sku": sku,
        "priceData": {"currency": "EUR", "price": eur(prezzo)},
        "description": descr_html,
        "visible": True
    }
    if brand:
        # In v1 brand è stringa semplice
        product["brand"] = brand

    body = {"product": product}
    _status, js = req("POST", "/stores/v1/products", body, ok=(200,201))
    pid = js.get("product", {}).get("id")
    if not pid:
        raise RuntimeError(f"Creazione prodotto senza id. Risposta: {js}")
    return pid

def patch_add_option(product_id: str):
    # Aggiungiamo SOLO l'opzione con 3 scelte. Nessuna gestione prezzi qui.
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
                        {"value": "PA", "description": "PAGAMENTO ANTICIPATO"},
                        {"value": "PR", "description": "PAGAMENTO RATEALE"}
                    ]
                }
            ]
        }
    }
    req("PATCH", f"/stores/v1/products/{product_id}", body, ok=(200,))

def load_csv(path: str):
    with open(path, "r", encoding="utf-8-sig", newline="") as fh:
        reader = csv.DictReader(fh, delimiter=";")
        expected = ["nome_articolo","prezzo_eur","sku","brand","descrizione","preorder_scadenza","eta"]
        missing = [c for c in expected if c not in reader.fieldnames]
        if missing:
            print(f"[WARN] CSV colonne mancanti: {missing}. Procedo comunque.")
        for row in reader:
            # Skippa righe palesemente vuote
            if not (row.get("sku") or "").strip() and not (row.get("nome_articolo") or "").strip():
                continue
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

            # 1) Aggiungo l'opzione con le 3 scelte (niente prezzi)
            try:
                patch_add_option(pid)
            except Exception as e:
                errors += 1
                print(f"[ERRORE] Opzioni {display}: {e}")
                continue

            # piccola attesa di grazia
            time.sleep(0.2)

            created += 1

        except Exception as e:
            errors += 1
            print(f"[ERRORE] Riga '{display}': {e}")

    print(f"[DONE] Creati/Aggiornati (base): {created}, Errori: {errors}")
    if errors:
        sys.exit(2)

if __name__ == "__main__":
    main()
