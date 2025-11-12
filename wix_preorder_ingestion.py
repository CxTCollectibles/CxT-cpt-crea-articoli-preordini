#!/usr/bin/env python3
import csv
import json
import os
import sys
import time
from decimal import Decimal, ROUND_HALF_UP
import requests
from html import escape

# ========== CONFIG ==========
CSV_PATH = os.getenv("CSV_PATH", "input/template_preordini_v7.csv")
API_BASE = os.getenv("WIX_API_BASE", "https://www.wixapis.com")
AUTH_HEADER = os.getenv("WIX_AUTH_HEADER")  # API Key pura oppure "Bearer <token>"
SITE_ID = os.getenv("WIX_SITE_ID")          # obbligatorio se usi API Key
# ===========================

def _money(val):
    return Decimal(val).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)

def _headers():
    """
    Costruisce gli header per Wix.
    - Authorization: prende il valore esatto che hai messo in WIX_AUTH_HEADER.
      Se è API Key pura, Wix la accetta senza 'Bearer'.
      Se è OAuth, tu gli passi 'Bearer ...' già completo.
    - wix-site-id: obbligatorio con API Key.
    """
    if not AUTH_HEADER:
        raise RuntimeError("Variabile WIX_AUTH_HEADER mancante.")
    h = {
        "Authorization": AUTH_HEADER,
        "Content-Type": "application/json",
        "Accept": "application/json"
    }
    # Se hai messo SITE_ID, aggiungo l’header (necessario con API Key)
    if SITE_ID:
        h["wix-site-id"] = SITE_ID
    return h

def read_csv(path):
    with open(path, "r", encoding="utf-8-sig", newline="") as fh:
        rdr = csv.DictReader(fh, delimiter=";")
        cols = rdr.fieldnames or []
        required = ["nome_articolo","prezzo_eur","sku","brand","categoria","descrizione","preorder_scadenza","eta"]
        missing = [c for c in required if c not in cols]
        if missing:
            raise RuntimeError(f"CSV mancano colonne: {missing}")
        for i, row in enumerate(rdr, start=2):
            yield i, row

def get_collections():
    # Tentativo 1: GET
    url = f"{API_BASE}/stores/v1/collections?limit=100"
    try:
        r = requests.get(url, headers=_headers(), timeout=30)
        if r.status_code == 200:
            data = r.json()
            cols = data.get("collections", [])
            return {c["name"].strip().lower(): c["id"] for c in cols}
    except Exception:
        pass
    # Tentativo 2: POST /query
    url = f"{API_BASE}/stores/v1/collections/query"
    body = {"query": {"paging": {"limit": 100}}}
    r = requests.post(url, headers=_headers(), data=json.dumps(body), timeout=30)
    r.raise_for_status()
    cols = r.json().get("collections", [])
    return {c["name"].strip().lower(): c["id"] for c in cols}

def query_product_by_sku(sku):
    url = f"{API_BASE}/stores/v1/products/query"
    body = {"query": {"filter": {"sku": {"$eq": str(sku)}}, "paging": {"limit": 1}}}
    r = requests.post(url, headers=_headers(), data=json.dumps(body), timeout=30)
    if r.status_code == 200:
        items = r.json().get("products", [])
        return items[0] if items else None
    if r.status_code == 400:
        return None
    r.raise_for_status()
    return None

def create_product(payload):
    url = f"{API_BASE}/stores/v1/products"
    r = requests.post(url, headers=_headers(), data=json.dumps({"product": payload}), timeout=30)
    r.raise_for_status()
    return r.json().get("product")

def update_product(prod_id, patch):
    url = f"{API_BASE}/stores/v1/products/{prod_id}"
    r = requests.patch(url, headers=_headers(), data=json.dumps({"product": {"id": prod_id, **patch}}), timeout=30)
    r.raise_for_status()
    return r.json().get("product")

def update_variants(prod_id, variants):
    url = f"{API_BASE}/stores/v1/products/{prod_id}/variants"
    body = {"variants": variants}
    r = requests.patch(url, headers=_headers(), data=json.dumps(body), timeout=30)
    r.raise_for_status()
    return r.json()

def add_to_collection(collection_id, product_id):
    url = f"{API_BASE}/stores/v1/collections/{collection_id}/products/add"
    body = {"productIds": [product_id]}
    r = requests.post(url, headers=_headers(), data=json.dumps(body), timeout=30)
    return r.status_code in (200, 204)

def build_description(deadline, eta, descr):
    # Rigo 1: Preorder Deadline, Rigo 2: ETA, riga vuota, poi descrizione
    pieces = []
    if (deadline or "").strip():
        pieces.append(f"Preorder Deadline: {escape(deadline.strip())}")
    if (eta or "").strip():
        pieces.append(f"ETA: {escape(eta.strip())}")
    pieces.append("")  # riga vuota
    base = descr or ""
    descr_html = escape(base).replace("\n", "<br>")
    body = "<br>".join(pieces) + (f"<br>{descr_html}" if descr_html else "")
    return f"<div><p>{body}</p></div>"

def ensure_options(prod_id, price_eur, base_sku):
    """
    Opzione unica:
      PREORDER PAYMENTS OPTIONS* :
        - ANTICIPO/SALDO  -> prezzo = 30% del listino
        - PAGAMENTO ANTICIPATO -> price pieno con discountedPrice 5% in meno
    """
    # Prezzi
    full_price = _money(price_eur)
    deposito = _money(full_price * Decimal("0.30"))
    anticipato = _money(full_price * Decimal("0.95"))

    # 1) aggiorno il prodotto: abilito manageVariants + definisco productOptions con VALUE corretti
    url = f"{API_BASE}/stores/v1/products/{prod_id}"
    productOptions = [{
        "name": "PREORDER PAYMENTS OPTIONS*",
        "choices": [
            {
                "value": "ANTICIPO/SALDO",
                "description": f"Acconto 30%: {deposito} €. Saldo all'arrivo."
            },
            {
                "value": "PAGAMENTO ANTICIPATO",
                "description": f"Pagamento completo con 5% di sconto. Prezzo: {anticipato} €"
            }
        ]
    }]
    patch = {
        "manageVariants": True,
        "productOptions": productOptions
    }
    r2 = requests.patch(url, headers=_headers(),
                        data=json.dumps({"product": {"id": prod_id, **patch}}),
                        timeout=30)
    r2.raise_for_status()

    # 2) aggiorno le varianti coerenti all’opzione
    variants = [
        {
            "sku": f"{base_sku}-DEP" if base_sku else f"SKU{int(time.time())}-DEP",
            "choices": {"PREORDER PAYMENTS OPTIONS*": "ANTICIPO/SALDO"},
            "priceData": {"currency": "EUR", "price": float(deposito)}
        },
        {
            "sku": f"{base_sku}-ADV" if base_sku else f"SKU{int(time.time())}-ADV",
            "choices": {"PREORDER PAYMENTS OPTIONS*": "PAGAMENTO ANTICIPATO"},
            "priceData": {"currency": "EUR", "price": float(full_price), "discountedPrice": float(anticipato)}
        }
    ]
    update_variants(prod_id, variants)

def main():
    print(f"[INFO] CSV: {CSV_PATH}")

    # Precheck e categorie
    try:
        collections_map = get_collections()
        if collections_map:
            coll_list = ", ".join(sorted(collections_map.keys()))
            print(f"[INFO] Categorie caricate: {coll_list}")
    except Exception as e:
        collections_map = {}
        print(f"[WARN] Lettura categorie fallita: {e}")

    created = 0
    updated = 0
    errors = 0

    for rownum, row in read_csv(CSV_PATH):
        name = (row.get("nome_articolo") or "").strip()
        if not name:
            print(f"[SKIP] Riga {rownum}: nome_articolo mancante.")
            continue

        if len(name) > 80:
            print("[WARN] Nome > 80 caratteri, troncato.")
            name = name[:80]

        sku = (row.get("sku") or "").strip()
        brand = (row.get("brand") or "").strip()
        categoria = (row.get("categoria") or "").strip().lower()
        descr_raw = row.get("descrizione") or ""
        deadline = row.get("preorder_scadenza") or ""
        eta = row.get("eta") or ""
        prezzo_str = (row.get("prezzo_eur") or "").replace(",", ".").strip()

        try:
            price_eur = _money(prezzo_str)
        except Exception:
            print(f"[SKIP] Riga {rownum}: prezzo_eur non valido.")
            continue

        print(f"[WORK] Riga {rownum}: {name} (SKU={sku})")

        descr_html = build_description(deadline, eta, descr_raw)

        # Cerca prodotto per SKU
        prod = None
        if sku:
            try:
                prod = query_product_by_sku(sku)
            except Exception as e:
                print(f"[WARN] Query SKU fallita: {e}")

        base_payload = {
            "name": name,
            "productType": "physical",
            "visible": True,
            "sku": sku if sku else None,
            "brand": brand if brand else None,
            "ribbon": "PREORDER",
            "priceData": {"currency": "EUR", "price": float(price_eur)},
            "description": descr_html,
            "manageVariants": True
        }
        base_payload = {k: v for k, v in base_payload.items() if v is not None}

        try:
            if not prod:
                newp = create_product(base_payload)
                prod_id = newp["id"]
                created += 1
                print(f"[NEW] {name} (id={prod_id})")
            else:
                prod_id = prod["id"]
                update_product(prod_id, base_payload)
                updated += 1
                print(f"[UPD] {name} (id={prod_id})")

            # Varianti
            ensure_options(prod_id, price_eur, sku)

            # Categoria
            if categoria and collections_map:
                coll_id = collections_map.get(categoria)
                if coll_id:
                    add_to_collection(coll_id, prod_id)
                else:
                    for k, v in collections_map.items():
                        if k.startswith(categoria):
                            add_to_collection(v, prod_id)
                            break

        except requests.HTTPError as he:
            errors += 1
            detail = he.response.text if getattr(he, "response", None) else str(he)
            print(f"[ERRORE] Riga {rownum} '{name}': {detail}")
        except Exception as e:
            errors += 1
            print(f"[ERRORE] Riga {rownum} '{name}': {e}")

    print(f"[DONE] Creati: {created}, Aggiornati: {updated}, Errori: {errors}")
    if created == 0 and updated == 0:
        sys.exit(2)

if __name__ == "__main__":
    main()
