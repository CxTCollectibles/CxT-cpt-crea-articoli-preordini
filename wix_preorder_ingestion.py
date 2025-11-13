#!/usr/bin/env python3
import os, sys, csv, json, time
import requests

BASE = "https://www.wixapis.com"

# ====== CONFIG ======
CSV_REQUIRED = [
    "nome_articolo","prezzo_eur","sku","brand","categoria",
    "descrizione","preorder_scadenza","eta"
]
OPTION_TITLE = "PREORDER PAYMENTS OPTIONS*"
CHOICE_AS = "ANTICIPO/SALDO"
CHOICE_PA = "PAGAMENTO ANTICIPATO"
RIBBON = "PREORDER"
CURRENCY = "EUR"

# ====== HTTP ======
def headers():
    site = os.environ.get("WIX_SITE_ID","").strip()
    key  = os.environ.get("WIX_API_KEY","").strip()
    if not site or not key:
        raise RuntimeError("Variabili WIX_API_KEY e/o WIX_SITE_ID mancanti.")
    return {
        "Authorization": key,
        "wix-site-id": site,
        "Content-Type": "application/json"
    }

def req(method, path, **kw):
    url = BASE + path
    r = requests.request(method, url, headers=headers(), timeout=30, **kw)
    if r.status_code >= 400:
        msg = ""
        try:
            msg = r.json()
        except Exception:
            msg = r.text
        raise requests.HTTPError(f"{r.status_code} {msg}")
    if r.content and r.headers.get("Content-Type","").startswith("application/json"):
        return r.json()
    return {}

# ====== UTIL ======
def trunc_name(name):
    return name if len(name) <= 80 else name[:80]

def parse_price(s):
    s = (s or "").replace(",",".").strip()
    return round(float(s), 2)

def read_csv(csv_path):
    with open(csv_path, "r", encoding="utf-8-sig", newline="") as fh:
        rdr = csv.DictReader(fh, delimiter=";")
        missing = [c for c in CSV_REQUIRED if c not in rdr.fieldnames]
        if missing:
            raise RuntimeError(f"CSV mancano colonne: {missing}")
        for row in rdr:
            yield row

def fmt_description(deadline, eta, descr):
    top = []
    if deadline: top.append(f"Preorder deadline: {deadline}")
    if eta:      top.append(f"ETA: {eta}")
    above = "<br>".join(top)
    body = (descr or "").strip()
    # riga vuota tra header e descrizione
    if above and body:
        html = f"<div><p>{above}</p><p></p><p>{body.replace('\\n','<br>')}</p></div>"
    elif above:
        html = f"<div><p>{above}</p></div>"
    else:
        html = f"<div><p>{body.replace('\\n','<br>')}</p></div>" if body else ""
    return html

def get_collections():
    out = {}
    data = req("GET", "/stores/v1/collections?limit=100")
    for c in data.get("collections", []):
        out[(c.get("name") or "").strip().lower()] = c.get("id")
    return out

def find_product_by_sku(sku):
    # query v1
    body = {"query": {"filter": {"sku": sku}, "paging": {"limit": 1}}}
    data = req("POST", "/stores/v1/products/query", json=body)
    items = data.get("products", [])
    return items[0] if items else None

def create_or_update_product(row):
    name = trunc_name(row["nome_articolo"].strip())
    price = parse_price(row["prezzo_eur"])
    sku = row["sku"].strip()
    brand = row["brand"].strip()
    categoria = row["categoria"].strip()
    deadline = (row.get("preorder_scadenza") or "").strip()
    eta = (row.get("eta") or "").strip()
    descr = (row.get("descrizione") or "").strip()

    desc_html = fmt_description(deadline, eta, descr)

    # Pre-esistente?
    existing = None
    try:
        existing = find_product_by_sku(sku)
    except Exception as e:
        print(f"[WARN] Query SKU fallita {sku}: {e}")

    product_payload = {
        "name": name,
        "slug": None,
        "productType": "physical",
        "sku": sku,
        "visible": True,
        "brand": brand,
        "ribbon": RIBBON,
        "price": {"currency": CURRENCY, "price": price},
        "description": desc_html,
        "manageVariants": True,
        "productOptions": [
            {
                "name": OPTION_TITLE,
                "type": "dropDown",
                "choices": [
                    {"value": CHOICE_AS, "description": CHOICE_AS},
                    {"value": CHOICE_PA, "description": CHOICE_PA}
                ]
            }
        ],
    }

    if existing:
        pid = existing["id"]
        try:
            req("PATCH", f"/stores/v1/products/{pid}", json={"product": product_payload})
            print(f"[UPD] {name} (SKU={sku})")
        except Exception as e:
            raise RuntimeError(f"PATCH /products/{pid} fallita: {e}")
    else:
        try:
            data = req("POST", "/stores/v1/products", json={"product": product_payload})
            pid = data["product"]["id"]
            print(f"[NEW] {name} (SKU={sku})")
        except Exception as e:
            raise RuntimeError(f"POST /products fallita: {e}")

    # Varianti: prezzi
    try:
        v = req("GET", f"/stores/v1/products/{pid}/variants?limit=100")
        variants = v.get("variants", [])
        id_as = id_pa = None
        for var in variants:
            # var["choices"] è una lista di dict { "option": title, "value": value }
            choices = {c.get("option"): c.get("value") for c in var.get("choices", [])}
            if choices.get(OPTION_TITLE) == CHOICE_AS:
                id_as = var["id"]
            if choices.get(OPTION_TITLE) == CHOICE_PA:
                id_pa = var["id"]

        as_price = round(price * 0.30, 2)
        pa_price = round(price * 0.95, 2)

        patch_list = []
        if id_as:
            patch_list.append({"id": id_as, "priceData": {"price": as_price}})
        if id_pa:
            patch_list.append({"id": id_pa, "priceData": {"price": pa_price}})

        if patch_list:
            req("PATCH", f"/stores/v1/products/{pid}/variants", json={"variants": patch_list})
            print(f"[OK] Prezzi varianti aggiornati: {CHOICE_AS}={as_price} EUR, {CHOICE_PA}={pa_price} EUR")
        else:
            print(f"[WARN] Varianti non trovate per {name}.")
    except Exception as e:
        print(f"[WARN] Patch varianti fallita: {e}")

    # Collezione
    if categoria:
        try:
            collections = get_collections()
            cid = collections.get(categoria.strip().lower())
            if cid:
                try:
                    req("POST", f"/stores/v1/collections/{cid}/products/add", json={"productIds": [pid]})
                    print(f"[OK] Categoria assegnata: {categoria}")
                except Exception as e:
                    print(f"[WARN] Impossibile aggiungere a '{categoria}' (smart collection o permesso): {e}")
            else:
                print(f"[WARN] Collezione '{categoria}' non trovata.")
        except Exception as e:
            print(f"[WARN] Lettura collezioni fallita: {e}")

    return pid

def main():
    if len(sys.argv) < 2:
        print("Uso: wix_preorder_ingestion.py <path_csv>")
        sys.exit(2)
    csv_path = sys.argv[1]
    print(f"[INFO] CSV: {csv_path}")
    created = updated = errors = 0

    for row in read_csv(csv_path):
        name = trunc_name(row["nome_articolo"].strip())
        sku = row["sku"].strip()
        try:
            pid = create_or_update_product(row)
            if pid:
                # stima: se esisteva è update, altrimenti new (log già stampato)
                updated += 1
        except Exception as e:
            errors += 1
            print(f"[ERRORE] '{name}': {e}")

    print(f"[DONE] Errori: {errors}")
    if errors:
        sys.exit(2)

if __name__ == "__main__":
    main()
