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

# Percentuale anticipo: di default 30%. Cambiabile da env.
try:
    DEPOSIT_PCT = float(os.environ.get("DEPOSIT_PCT", "0.30"))
except:
    DEPOSIT_PCT = 0.30

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
        # tenta di estrarre body testuale per log chiaro
        body = r.text
        raise RuntimeError(f"{method} {path} failed {r.status_code}: {body}")
    if r.text.strip() == "":
        return r.status_code, {}
    try:
        return r.status_code, r.json()
    except:
        return r.status_code, {}

def eur(v: float) -> float:
    # arrotondamento due decimali in stile e-commerce
    return round(float(v) + 1e-9, 2)

def build_description(preorder_deadline: str, eta: str, descr_it: str) -> str:
    # Richiesta: riga "Preorder Deadline" e "ETA", una riga vuota, poi descrizione.
    # Evito backslash in f-string usando variabili intermedie.
    pd = preorder_deadline.strip() if preorder_deadline else ""
    et = eta.strip() if eta else ""
    di = descr_it.strip() if descr_it else ""
    di_html = di.replace("\n", "<br>")

    parts = []
    if pd:
        parts.append(f"<p><strong>Preorder Deadline:</strong> {pd}</p>")
    if et:
        parts.append(f"<p><strong>ETA:</strong> {et}</p>")
    # riga vuota
    parts.append("<p>&nbsp;</p>")
    if di_html:
        parts.append(f"<p>{di_html}</p>")
    return "\n".join(parts)

def create_product(row: Dict[str, str]) -> Tuple[str, str]:
    """
    Crea un prodotto base (senza varianti).
    Ritorna (product_id, revision) dove revision qui non serve per v1.
    """
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
    except:
        prezzo = 0.0

    descr_html = build_description(preorder_scadenza, eta, descr)

    body = {
        "product": {
            "name": nome[:80] if nome else sku,
            "productType": "physical",
            "sku": sku,
            "priceData": {
                "currency": "EUR",
                "price": eur(prezzo)
            },
            # Il campo brand funziona come oggetto con name
            "brand": {"name": brand} if brand else None,
            "description": descr_html,
            "visible": True
        }
    }
    # rimuovi chiavi None che Wix odia
    body["product"] = {k: v for k, v in body["product"].items() if v is not None}

    status, js = req("POST", "/stores/v1/products", body, ok=(200,201))
    pid = js.get("product", {}).get("id")
    if not pid:
        raise RuntimeError(f"Creazione prodotto senza id. Risposta: {js}")
    return pid, js.get("product", {}).get("revision", "")

def patch_options_and_variants(product_id: str, sku_base: str, full_price: float):
    """
    Imposta:
      - 1 opzione: PREORDER PAYMENTS OPTIONS*
      - 2 scelte: ANTICIPO/SALDO, PAGAMENTO ANTICIPATO
      - 2 varianti con prezzi calcolati
    NOTA: per v1, le scelte richiedono 'value' (ID interno) e 'description' (testo visibile).
          Le varianti vanno referenziate con la 'description' della scelta.
    """
    deposit = eur(full_price * DEPOSIT_PCT)

    option_name = "PREORDER PAYMENTS OPTIONS*"
    choice_deposit_desc = "ANTICIPO/SALDO"
    choice_deposit_val = "AS"
    choice_full_desc = "PAGAMENTO ANTICIPATO"
    choice_full_val = "PA"

    body = {
        "product": {
            "manageVariants": True,
            "productOptions": [
                {
                    "name": option_name,
                    "type": "drop_down",
                    "choices": [
                        {
                            "value": choice_deposit_val,
                            "description": choice_deposit_desc
                        },
                        {
                            "value": choice_full_val,
                            "description": choice_full_desc
                        }
                    ]
                }
            ],
            "variants": [
                {
                    # Attenzione: qui Wix si aspetta un OBJECT { "<nome_opzione>": "<description scelta>" }
                    "choices": {
                        option_name: choice_deposit_desc
                    },
                    "priceData": {
                        "currency": "EUR",
                        "price": deposit
                    },
                    "visible": True,
                    "sku": f"{sku_base}-AS"
                },
                {
                    "choices": {
                        option_name: choice_full_desc
                    },
                    "priceData": {
                        "currency": "EUR",
                        "price": eur(full_price)
                    },
                    "visible": True,
                    "sku": f"{sku_base}-PA"
                }
            ]
        }
    }
    # PATCH prodotto con opzioni e varianti
    req("PATCH", f"/stores/v1/products/{product_id}", body, ok=(200,))

def load_csv(path: str):
    # Expect utf-8-sig e separatore ';' come da XLS V7
    with open(path, "r", encoding="utf-8-sig", newline="") as fh:
        reader = csv.DictReader(fh, delimiter=";")
        # Mappatura minima: nomi già in V7
        required = ["nome_articolo", "prezzo_eur", "sku", "brand", "descrizione", "preorder_scadenza", "eta"]
        missing = [c for c in required if c not in reader.fieldnames]
        if missing:
            print(f"[WARN] CSV colonne mancanti: {missing}. Procedo dove posso.")
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
        except:
            prezzo = 0.0

        display = nome[:80] if nome else sku
        print(f"[WORK] {display} (SKU={sku})")

        try:
            # 1) crea base
            pid, _rev = create_product(row)
            print(f"[NEW] Creato {sku} -> {pid}")

            # 2) patch opzioni + varianti con prezzi
            try:
                patch_options_and_variants(pid, sku, prezzo)
            except Exception as e:
                errors += 1
                print(f"[ERRORE] Varianti {display}: {e}")

            created += 1
            # pausa minima per non stressare l'API
            time.sleep(0.2)

        except Exception as e:
            errors += 1
            print(f"[ERRORE] Riga '{display}': {e}")

    print(f"[DONE] Creati/Aggiornati (base): {created}, Errori: {errors}")
    if errors:
        sys.exit(2)

if __name__ == "__main__":
    main()
