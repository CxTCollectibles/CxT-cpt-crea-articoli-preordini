#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os, sys, csv, time, math, re, html
import json
import argparse
import requests
from urllib.parse import urljoin, urlparse
from bs4 import BeautifulSoup

WIX_API_KEY  = os.getenv("WIX_API_KEY", "").strip()
WIX_SITE_ID  = os.getenv("WIX_SITE_ID", "").strip()  # ATTENZIONE: deve essere il metaSiteId (quello che hai verificato con HTTP 200)
USER_AGENT   = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36"

# Endpoint base
STORES_BASE   = "https://www.wixapis.com/stores/v1"
STORES_V3     = "https://www.wixapis.com/stores/v3"
READER_BASE   = "https://www.wixapis.com/stores-reader/v1"
CATEGORIES_API= "https://www.wixapis.com/categories/v1"
MEDIA_API     = "https://www.wixapis.com/site-media/v1"

# AppId Wix Stores per Categories API (serve quando aggiungiamo item a una categoria)
WIX_STORES_APP_ID = "215238eb-22a5-4c36-9e7b-e7c08025e04e"

session = requests.Session()
session.headers.update({
    "Authorization": WIX_API_KEY,
    "wix-site-id": WIX_SITE_ID,
    "Content-Type": "application/json",
    "Accept": "application/json",
    "User-Agent": USER_AGENT
})

def die(msg, code=1):
    print(msg, file=sys.stderr)
    sys.exit(code)

def http(method, url, **kwargs):
    for attempt in range(3):
        r = session.request(method, url, timeout=30, **kwargs)
        if r.status_code in (429, 500, 502, 503, 504):
            time.sleep(1.5 * (attempt + 1))
            continue
        return r
    return r

def price_round(x):
    return round(float(x) + 1e-9, 2)

def parse_csv(path):
    rows = []
    with open(path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for i, row in enumerate(reader, start=2):
            rows.append((i, row))
    return rows

def abs_urls(base, urls):
    out = []
    for u in urls:
        try:
            if not u: 
                continue
            o = urlparse(u)
            out.append(u if o.scheme in ("http","https") else urljoin(base, u))
        except:
            pass
    return out

# ---------- SCRAPER DESCRIZIONE + IMMAGINI ----------

IMG_EXT = (".jpg",".jpeg",".png",".webp",".gif",".jfif")

def scrape_product_page(url):
    """Ritorna (descrizione_html, lista_url_immagini)"""
    try:
        r = requests.get(url, headers={"User-Agent": USER_AGENT}, timeout=30)
        if r.status_code != 200:
            return None, []
    except:
        return None, []
    base = "{uri.scheme}://{uri.netloc}/".format(uri=urlparse(url))
    soup = BeautifulSoup(r.text, "lxml")

    # 1) DESCRIZIONE: euristiche comuni
    desc = None

    # blocchi classici
    candidates = [
        {"name":"div", "attrs":{"class": re.compile("(product[-_ ](desc|description)|description|product-info)", re.I)}},
        {"name":"section", "attrs":{"class": re.compile("(description|details)", re.I)}},
        {"name":"div", "attrs":{"id": re.compile("description", re.I)}},
        {"name":"article", "attrs":{}},
    ]
    for c in candidates:
        el = soup.find(c.get("name"), c.get("attrs"))
        if el and el.get_text(strip=True):
            # spesso sotto le foto; prendi primi <p> sostanziosi
            ps = [p for p in el.find_all(["p","li"]) if p.get_text(strip=True)]
            if ps:
                chunk = " ".join([str(p) for p in ps[:6]])
                desc = chunk
                break

    # fallback: meta description
    if not desc:
        meta = soup.find("meta", attrs={"name":"description"}) or soup.find("meta", attrs={"property":"og:description"})
        if meta and meta.get("content"):
            desc = f"<p>{html.escape(meta['content'])}</p>"

    # 2) IMMAGINI: prova galleria, poi og:image, poi tutte le <img> “grandi”
    imgs = []

    # gallerie
    for sel in [
        ("div", {"class": re.compile("(gallery|product[-_ ]images|carousel|slick|swiper)", re.I)}),
        ("ul", {"class": re.compile("(thumb|gallery)", re.I)}),
    ]:
        gal = soup.find(*sel)
        if gal:
            for im in gal.find_all("img"):
                src = im.get("src") or im.get("data-src") or im.get("data-large_image") or im.get("data-zoom-image")
                if src:
                    imgs.append(src)

    # og:image
    og = soup.find("meta", attrs={"property":"og:image"})
    if og and og.get("content"):
        imgs.append(og["content"])

    # tutte le img, ma filtra piccole/icona
    for im in soup.find_all("img"):
        src = im.get("src") or im.get("data-src")
        if not src:
            continue
        w = (im.get("width") or "").strip()
        h = (im.get("height") or "").strip()
        if w.isdigit() and h.isdigit():
            if int(w) < 400 and int(h) < 400:
                continue
        imgs.append(src)

    imgs = abs_urls(base, imgs)
    # filtra per estensione e dedup
    seen = set()
    clean = []
    for u in imgs:
        low = u.lower().split("?")[0]
        if not low.endswith(IMG_EXT):
            continue
        if u in seen:
            continue
        seen.add(u)
        clean.append(u)
    return desc, clean[:15]

# ---------- MEDIA MANAGER UPLOAD ----------

def media_bulk_import(image_urls, folder_path="media-root/preordini"):
    """
    Usa Bulk Import File per importare URL esterni nella Media Manager.
    Ritorna una lista di dizionari {fileId, displayName}
    """
    if not image_urls:
        return []

    payload = {
        "files": [
            {
                "url": u,
                "displayName": os.path.basename(urlparse(u).path) or f"image_{i}.jpg",
                # Suggeriamo il mimeType se manca l'estensione chiara
                # "mimeType": "image/jpeg",
                "filePath": folder_path
            } for i, u in enumerate(image_urls)
        ]
    }
    r = http("POST", f"{MEDIA_API}/files/bulk-import-file", data=json.dumps(payload))
    if r.status_code != 200:
        print(f"[WARN] Bulk Import fallito {r.status_code}: {r.text}")
        return []

    data = r.json()
    # data["files"] lista file con .id e operationStatus. Aspetta READY.
    results = []
    for f in data.get("files", []):
        # poll semplice se non READY
        fid = f.get("id")
        status = (f.get("operationStatus") or "").upper()
        tries = 0
        while status not in ("READY","COMPLETED") and tries < 6 and fid:
            time.sleep(1.0)
            q = http("GET", f"{MEDIA_API}/files/{fid}")
            if q.status_code == 200:
                status = (q.json().get("file", {}).get("operationStatus") or "").upper()
            else:
                break
            tries += 1
        if fid:
            results.append({"fileId": fid, "displayName": f.get("displayName")})
    return results

def add_media_to_product(product_id, imported_files):
    if not imported_files:
        return
    body = {"mediaItems": [{"fileId": f["fileId"]} for f in imported_files]}
    r = http("POST", f"{STORES_BASE}/products/{product_id}/media", data=json.dumps(body))
    if r.status_code != 200:
        print(f"[WARN] Add Product Media fallita {r.status_code}: {r.text}")

# ---------- CATEGORIE ----------

def query_all_categories():
    # recupera tutte le categorie del sito
    payload = {"query": {}}
    cats = []
    r = http("POST", f"{CATEGORIES_API}/categories/query", data=json.dumps(payload))
    if r.status_code != 200:
        print(f"[WARN] Query Categories {r.status_code}: {r.text}")
        return {}
    data = r.json()
    cats.extend(data.get("categories", []))
    # TODO: se serve paginazione, aggiungerla
    by_name = {}
    for c in cats:
        nm = (c.get("name") or "").strip().lower()
        if nm:
            by_name[nm] = c.get("id")
    return by_name

def add_product_to_category(product_id, category_name):
    if not category_name:
        return
    cmap = query_all_categories()
    cid = cmap.get(category_name.strip().lower())
    if not cid:
        print(f"[WARN] Categoria '{category_name}' non trovata, salto assegnazione.")
        return
    body = {
        "items": [
            {"catalogItemId": product_id, "appId": WIX_STORES_APP_ID}
        ]
    }
    r = http("POST", f"{CATEGORIES_API}/bulk/categories/{cid}/add-items", data=json.dumps(body))
    if r.status_code != 200:
        print(f"[WARN] Add to Category fallita {r.status_code}: {r.text}")

# ---------- CREAZIONE/AGGIORNAMENTO PRODOTTO ----------

def create_product(payload):
    r = http("POST", f"{STORES_BASE}/products", data=json.dumps(payload))
    return r

def make_product_payload(row):
    name   = (row.get("Nome articolo") or "").strip()
    price  = price_round(row.get("Prezzo") or 0)
    sku    = (row.get("SKU") or "").strip()
    brand  = (row.get("Brand (seleziona)") or row.get("Brand") or "").strip()
    weight = float(row.get("Peso (kg)") or 0) if str(row.get("Peso (kg)") or "").strip() else None
    ean    = (row.get("GTIN/EAN") or "").strip()
    categoria = (row.get("Categoria (seleziona)") or row.get("Categoria") or "").strip()

    # Descrizione: override se presente, altrimenti scraping
    distributor_url = (row.get("URL pagina distributore") or "").strip()
    desc_override   = (row.get("Descrizione override (opzionale)") or "").strip()
    deadline = (row.get("Scadenza preordine (gg/mm/aaaa)") or "").strip()
    eta      = (row.get("ETA (mm/aaaa o gg/mm/aaaa)") or "").strip()
    extra_imgs = [u.strip() for u in (row.get("URL immagini extra (separate da |) [opzionale]") or "").split("|") if u.strip()]

    scraped_desc, scraped_imgs = (None, [])
    if distributor_url:
        d, imgs = scrape_product_page(distributor_url)
        scraped_desc, scraped_imgs = d, imgs

    # Preorder banner inline a inizio descrizione (se forniti deadline/eta)
    preorder_line = ""
    if deadline or eta:
        parts = []
        if deadline:
            parts.append(f"Scadenza preordine: {html.escape(deadline)}")
        if eta:
            parts.append(f"ETA: {html.escape(eta)}")
        preorder_line = f"<p><strong>PREORDER</strong> — {' — '.join(parts)}</p>"

    description_html = desc_override or scraped_desc or ""
    description_html = (preorder_line + (description_html or "")) or preorder_line or ""

    # Opzioni pagamento e varianti gestite
    base_price = price
    price_anticipo = price_round(base_price * 0.30)
    price_anticipato = price_round(base_price * 0.95)

    variants = [
        {
            "choices": {"Pagamento": "ANTICIPO/SALDO"},
            "priceData": {"price": price_anticipo},
            "sku": f"{sku}-AS" if sku else ""
        },
        {
            "choices": {"Pagamento": "PAGAMENTO ANTICIPATO"},
            "priceData": {"price": price_anticipato},
            "sku": f"{sku}-PA" if sku else ""
        }
    ]

    product_options = [
        {
            "name": "Pagamento",
            "choices": [
                {"value": "ANTICIPO/SALDO", "description": "ANTICIPO/SALDO"},
                {"value": "PAGAMENTO ANTICIPATO", "description": "PAGAMENTO ANTICIPATO"}
            ]
        }
    ]

    payload = {
        "name": name,
        "productType": "physical",
        "visible": True,
        "description": description_html[:7900],  # limite 8000
        "priceData": {"price": base_price},  # prezzo "listino"
        "sku": sku or None,
        "weight": weight if weight is not None else None,
        "brand": brand or None,
        "ribbon": "PREORDER",
        "manageVariants": True,
        "productOptions": product_options,
        "variants": variants,
    }

    # GTIN/EAN nei custom fields se necessario: per ora mettiamo in additionalInfoSections
    if ean:
        payload.setdefault("additionalInfoSections", []).append({
            "title": "GTIN/EAN",
            "description": html.escape(ean)
        })

    return payload, scraped_imgs, extra_imgs, categoria

def run(csv_path):
    # precheck API
    test = http("POST", f"{STORES_BASE}/products/query", data=json.dumps({"query": {"paging": {"limit": 1}}}))
    if test.status_code != 200:
        die(f"[ERRORE] API non valide o permessi insufficienti: {test.status_code} {test.text}")

    rows = parse_csv(csv_path)
    created = 0
    for line_no, row in rows:
        name = (row.get("Nome articolo") or "").strip()
        try:
            payload, scraped_imgs, extra_imgs, categoria = make_product_payload(row)
            r = create_product(payload)
            if r.status_code != 200:
                print(f"[ERRORE] Riga {line_no} '{name}': POST {STORES_BASE}/products failed {r.status_code}: {r.text}")
                continue
            product = r.json().get("product") or {}
            pid = product.get("id")
            if not pid:
                print(f"[ERRORE] Riga {line_no} '{name}': prodotto senza id")
                continue

            # IMMAGINI: priorità immagini da pagina, poi extra
            image_urls = scraped_imgs + [u for u in extra_imgs if u]
            imported = media_bulk_import(image_urls)
            if imported:
                add_media_to_product(pid, imported)
            else:
                print(f"[WARN] Riga {line_no} '{name}': nessuna immagine importata")

            # Categoria
            if categoria:
                add_product_to_category(pid, categoria)

            created += 1
            # log variante per conferma
            print(f"[OK] Creato '{name}' ({pid}). Varianti: ANTICIPO/SALDO={payload['variants'][0]['priceData']['price']} | PAGAMENTO ANTICIPATO={payload['variants'][1]['priceData']['price']}")
        except Exception as e:
            print(f"[ERRORE] Riga {line_no} '{name}': {e}")
            continue

    if created == 0:
        die("[ERRORE] Nessun prodotto creato.", 2)
    print(f"[FINE] Prodotti creati: {created}")

if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--csv", required=True, help="Percorso del CSV")
    args = ap.parse_args()
    if not os.path.exists(args.csv):
        die(f"File non trovato: {args.csv}")
    run(args.csv)
