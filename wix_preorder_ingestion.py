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
# Inserisci qui il tuo header di auth esattamente come fai già nel tuo workflow
AUTH_HEADER = os.getenv("WIX_AUTH_HEADER")  # es: "Bearer eyJ..." oppure "Authorization": "xxxxx"
# ===========================

def _money(val):
    return Decimal(val).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)

def _headers():
    # Se usi "Authorization: Bearer ..." metti in H["Authorization"] = AUTH_HEADER
    # Se usi chiave diversa, adegua QUI. Mantengo content-type/accept.
    if not AUTH_HEADER:
        raise RuntimeError("Variabile WIX_AUTH_HEADER mancante.")
    return {
        "Authorization": AUTH_HEADER,
        "Content-Type": "application/json",
        "Accept": "application/json"
    }

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
    # Prova GET, se 404 ripiega su POST query
    url = f"{API_BASE}/stores/v1/collections?limit=100"
    try:
        r = requests.get(url, headers=_headers(), timeout=30)
        if r.status_code == 200:
            data = r.json()
            cols = data.get("collections", [])
            return {c["name"].strip().lower(): c["id"] for c in cols}
    except Exception:
        pass
    # Fallback: POST query
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
    # Alcuni siti restituiscono 400 su filter sku, fallback per nome esatto
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
    # Sovrascrive i variants: devono combaciare con le productOptions già presenti
    url = f"{API_BASE}/stores/v1/products/{prod_id}/variants"
    body = {"variants": variants}
    r = requests.patch(url, headers=_headers(), data=json.dumps(body), timeout=30)
    r.raise_for_status()
    return r.json()

def add_to_collection(collection_id, product_id):
    url = f"{API_BASE}/stores/v1/collections/{collection_id}/products/add"
    body = {"productIds": [product_id]}
    r = requests.post(url, headers=_headers(), data=json.dumps(body), timeout=30)
    if r.status_code in (200, 204):
        return True
    # Se è già presente, alcuni ambienti rispondono 400/409. Ignoro errori non-bloccanti.
    return False

def build_description(deadline, eta, descr):
    # Primo rigo: Preorder Deadline, secondo: ETA, poi riga vuota, poi descrizione
    pieces = []
    if deadline:
        pieces.append(f"Preorder Deadline: {escape(deadline.strip())}")
    if eta:
        pieces.append(f"ETA: {escape(eta.strip())}")
    # riga vuota
    pieces.append("")
    # descr
    base = descr or ""
    # evito backslash dentro f-string preparando prima:
    descr_html = escape(base).replace("\n", "<br>")
    body = "<br>".join(pieces) + f"<br>{descr_html}" if pieces else descr_html
    return f"<div><p>{body}</p></div>"

def ensure_options(prod_id, price_eur, base_sku):
    """
    Crea/aggiorna l'opzione unica:
      PREORDER PAYMENTS OPTIONS* :
        - ANTICIPO/SALDO  (30% del prezzo)
        - PAGAMENTO ANTICIPATO (95% del prezzo, con price sbarrato = 100%)
    """
    # Aggiorna il prodotto per attivare manageVariants e definire le options con VALUE giusti.
    url = f"{API_BASE}/stores/v1/products/{prod_id}"
    r = requests.get(url, headers=_headers(), timeout=30)
    r.raise_for_status()
    prod = r.json().get("product", {})

    # Calcoli prezzi
    full_price = _money(price_eur)
    deposito = _money(full_price * Decimal("0.30"))
    anticipato = _money(full_price * Decimal("0.95"))

    # Opzione e choices con VALUE corretti
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
    # Brand/ribbon/price non li tocco qui: vengono messi altrove.

    r2 = requests.patch(url, headers=_headers(),
                        data=json.dumps({"product": {"id": prod_id, **patch}}),
                        timeout=30)
    r2.raise_for_status()

    # Ora aggiorno TUTTE le varianti coerenti con le options
    variants = [
        {
            "sku": f"{base_sku}-DEP",
            "choices": {"PREORDER PAYMENTS OPTIONS*": "ANTICIPO/SALDO"},
            "priceData": {"currency": "EUR", "price": float(deposito)}
        },
        {
            "sku": f"{base_sku}-ADV",
            "choices": {"PREORDER PAYMENTS OPTIONS*": "PAGAMENTO ANTICIPATO"},
            "priceData": {"currency": "EUR", "price": float(full_price), "discountedPrice": float(anticipato)}
        }
    ]
    update_variants(prod_id, variants)

def main():
    print(f"[INFO] CSV: {CSV_PATH}")
    # Precheck API
    try:
        _ = get_collections()
        print("[PRECHECK] API ok.")
    except Exception as e:
        print(f"[WARN] Precheck collezioni fallito (non bloccante): {e}")

    # Carico mappa collezioni
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

        # Troncatura nome a 80 char
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

        # Costruisco descrizione HTML
        descr_html = build_description(deadline, eta, descr_raw)

        # Cerco esistenza per SKU
        prod = None
        if sku:
            try:
                prod = query_product_by_sku(sku)
            except Exception as e:
                print(f"[WARN] Query SKU fallita: {e}")

        # Payload base
        base_payload = {
            "name": name,
            "productType": "physical",
            "visible": True,
            "sku": sku if sku else None,
            "brand": brand if brand else None,
            "ribbon": "PREORDER",
            "priceData": {"currency": "EUR", "price": float(price_eur)},
            "description": descr_html,
            "manageVariants": True  # sempre abilitate
        }
        # ripulisci None
        base_payload = {k: v for k, v in base_payload.items() if v is not None}

        try:
            if not prod:
                # CREATE
                newp = create_product(base_payload)
                prod_id = newp["id"]
                created += 1
                print(f"[NEW] {name} (id={prod_id})")
            else:
                # UPDATE
                prod_id = prod["id"]
                update_product(prod_id, base_payload)
                updated += 1
                print(f"[UPD] {name} (id={prod_id})")

            # Garantisco l'opzione + variants coerenti
            ensure_options(prod_id, price_eur, sku if sku else f"SKU{int(time.time())}")

            # Aggancio categoria se presente
            if categoria and collections_map:
                coll_id = collections_map.get(categoria)
                if coll_id:
                    add_to_collection(coll_id, prod_id)
                else:
                    # fallback: prova match “starts with”
                    for k, v in collections_map.items():
                        if k.startswith(categoria):
                            add_to_collection(v, prod_id)
                            break

        except requests.HTTPError as he:
            errors += 1
            try:
                detail = he.response.text
            except Exception:
                detail = str(he)
            print(f"[ERRORE] Riga {rownum} '{name}': {detail}")
        except Exception as e:
            errors += 1
            print(f"[ERRORE] Riga {rownum} '{name}': {e}")

    print(f"[DONE] Creati: {created}, Aggiornati: {updated}, Errori: {errors}")
    if created == 0 and updated == 0:
        sys.exit(2)

if __name__ == "__main__":
    main()
