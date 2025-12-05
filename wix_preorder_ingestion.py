#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os, sys, csv, json, math, time
import requests

BASE = "https://www.wixapis.com"

# ========= Config da ENV =========
API_KEY      = os.getenv("WIX_API_KEY", "").strip()
SITE_ID      = os.getenv("WIX_SITE_ID", "").strip()
CSV_PATH     = os.getenv("CSV_PATH", "input/template_preordini_v7.csv").strip()
CURRENCY     = os.getenv("CURRENCY", "EUR").strip()

# Varianti prezzo
DEPOSIT_PERCENT         = float(os.getenv("DEPOSIT_PERCENT", "0.20"))  # 20%
RATE_INSTALLMENTS       = int(os.getenv("RATE_INSTALLMENTS", "3"))     # 3 rate
RATE_SURCHARGE_PERCENT  = float(os.getenv("RATE_SURCHARGE_PERCENT", "0.0"))

# Nome opzione e valori
OPT_NAME = "PREORDER PAYMENTS OPTIONS*"
CHOICE_ACCONTO  = "ANTICIPO/SALDO"
CHOICE_FULL     = "PAGAMENTO ANTICIPATO"
CHOICE_RATE     = "PAGAMENTO RATEALE"

# =================================

def die(msg, code=1):
    print(msg)
    sys.exit(code)

def headers():
    if not API_KEY or not SITE_ID:
        die("Errore: WIX_API_KEY o WIX_SITE_ID mancanti (secrets).")
    return {
        "Authorization": API_KEY,          # API Key raw (non Bearer)
        "wix-site-id": SITE_ID,
        "Content-Type": "application/json"
    }

def req(method, path, payload=None, params=None, expected=(200,201)):
    url = BASE + path
    r = requests.request(method, url, headers=headers(), json=payload, params=params, timeout=30)
    if r.status_code not in expected:
        raise RuntimeError(f"{method} {path} failed {r.status_code}: {r.text or ''}")
    return r.json() if r.text else {}

def clamp_name(name: str, maxlen=80) -> str:
    name = (name or "").strip()
    return name if len(name) <= maxlen else name[:maxlen]

def money_round(x: float) -> float:
    # Wix accetta due decimali
    return round(float(x) + 1e-9, 2)

def parse_price(value: str) -> float:
    if value is None: return 0.0
    s = str(value).replace("€","").replace(",", ".").strip()
    try:
        return float(s)
    except:
        return 0.0

def build_description(pre_deadline: str, eta: str, descr: str) -> str:
    pre_deadline = (pre_deadline or "").strip()
    eta = (eta or "").strip()
    descr = (descr or "").strip()

    descr_html = descr.replace("\n", "<br>")
    parts = []

    if pre_deadline:
        parts.append(f"<p><strong>Preorder Deadline:</strong> {pre_deadline} <em>Salvo esaurimento</em></p>")
    if eta:
        parts.append(f"<p><strong>ETA:</strong> {eta}</p>")

    # riga vuota di separazione
    parts.append("<br/>")

    if descr_html:
        parts.append(f"<p>{descr_html}</p>")

    return "".join(parts)

def create_product(row):
    name = clamp_name(row.get("nome_articolo",""))
    sku  = (row.get("sku","") or "").strip()
    brand = (row.get("brand","") or "").strip()

    price = parse_price(row.get("prezzo_eur"))
    if price <= 0:
        raise RuntimeError(f"Prezzo non valido per SKU={sku}")

    descr_html = build_description(row.get("preorder_scadenza") or row.get("preorder_deadline") or "",
                                   row.get("eta") or "",
                                   row.get("descrizione") or "")

    body = {
        "name": name,
        "productType": "physical",
        "sku": sku,
        "visible": True,
        "priceData": {
            "price": money_round(price),
            "currency": CURRENCY
        },
        "brand": brand,
        "manageVariants": True,
        "productOptions": [
            {
                "name": OPT_NAME,
                "choices": [
                    {"value": CHOICE_ACCONTO, "description": CHOICE_ACCONTO},
                    {"value": CHOICE_FULL,    "description": CHOICE_FULL},
                    {"value": CHOICE_RATE,    "description": CHOICE_RATE},
                ]
            }
        ],
        "description": descr_html
    }

    res = req("POST", "/stores/v1/products", payload=body, expected=(200,201))
    pid = res.get("id") or res.get("productId") or res.get("product", {}).get("id")
    if not pid:
        raise RuntimeError(f"Creazione prodotto riuscita ma ID non trovato. Risposta: {json.dumps(res)[:500]}")
    return pid, price

def update_variants_prices(product_id: str, base_price: float):
    deposit = money_round(max(1.0, base_price * DEPOSIT_PERCENT))
    full    = money_round(base_price)
    rate_total = base_price * (1.0 + RATE_SURCHARGE_PERCENT)
    rate_installment = money_round(max(1.0, rate_total / max(1, RATE_INSTALLMENTS)))

    # PATCH /stores/v1/products/{id}/variants
    body = {
        "variants": [
            {
                "choices": { OPT_NAME: CHOICE_ACCONTO },
                "priceData": { "price": deposit, "currency": CURRENCY }
            },
            {
                "choices": { OPT_NAME: CHOICE_FULL },
                "priceData": { "price": full, "currency": CURRENCY }
            },
            {
                "choices": { OPT_NAME: CHOICE_RATE },
                "priceData": { "price": rate_installment, "currency": CURRENCY }
            }
        ]
    }
    req("PATCH", f"/stores/v1/products/{product_id}/variants", payload=body, expected=(200,201))

def main():
    csv_path = CSV_PATH or (len(sys.argv) > 1 and sys.argv[1]) or "input/template_preordini_v7.csv"
    print(f"[INFO] CSV: {csv_path}")

    # Apri CSV
    with open(csv_path, "r", encoding="utf-8-sig", newline="") as fh:
        reader = csv.DictReader(fh, delimiter=";")
        # mappa colonne attese
        # nome_articolo;prezzo_eur;sku;brand;categoria;descrizione;preorder_scadenza;eta; ...
        expected = ["nome_articolo", "prezzo_eur", "sku", "brand", "descrizione"]
        for col in expected:
            if col not in reader.fieldnames:
                raise RuntimeError(f"CSV manca la colonna obbligatoria: {col}")

        created = 0
        updated = 0
        errors = 0

        for idx, row in enumerate(reader, start=2):
            name = clamp_name(row.get("nome_articolo",""))
            sku  = (row.get("sku","") or "").strip()
            print(f"[WORK] {name} (SKU={sku})")

            try:
                # Creazione prodotto con opzione e descrizione completa
                pid, base_price = create_product(row)
                print(f"[NEW] Creato {sku} -> {pid}")

                # Aggiorna varianti con prezzi corretti (3 scelte)
                update_variants_prices(pid, base_price)
                print(f"[OK] Varianti prezzo aggiornate per {sku}")

                created += 1

            except RuntimeError as e:
                msg = str(e)
                if "product.sku is not unique" in msg:
                    errors += 1
                    print(f"[SKIP] SKU duplicato {sku}. Salto (non gestisco update esistenti in questo run).")
                else:
                    errors += 1
                    print(f"[ERRORE] Riga {idx} '{name}': {msg}")
            except Exception as e:
                errors += 1
                print(f"[ERRORE] Riga {idx} '{name}': {e}")

        print(f"[DONE] Creati: {created}, Aggiornati: {updated}, Errori: {errors}")
        if errors > 0:
            sys.exit(2)

if __name__ == "__main__":
    main()
