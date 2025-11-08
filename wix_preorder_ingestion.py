#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os, sys, csv, time, re, html, json
import argparse
import requests
from urllib.parse import urljoin, urlparse
from bs4 import BeautifulSoup

# --- Config da env ---
WIX_API_KEY  = os.getenv("WIX_API_KEY", "").strip()
WIX_SITE_ID  = os.getenv("WIX_SITE_ID", "").strip()  # metaSiteId
USER_AGENT   = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome Safari"

# --- Endpoint ---
STORES_V1    = "https://www.wixapis.com/stores/v1"
CATEGORIES_V1= "https://www.wixapis.com/categories/v1"
MEDIA_V1     = "https://www.wixapis.com/site-media/v1"
WIX_STORES_APP_ID = "215238eb-22a5-4c36-9e7b-e7c08025e04e"

session = requests.Session()
session.headers.update({
    "Authorization": WIX_API_KEY,
    "wix-site-id": WIX_SITE_ID,
    "Content-Type": "application/json",
    "Accept": "application/json",
    "User-Agent": USER_AGENT
})

def fail(msg, code=1):
    print(msg, file=sys.stderr)
    sys.exit(code)

def http(method, url, **kw):
    for i in range(3):
        r = session.request(method, url, timeout=40, **kw)
        if r.status_code in (429,500,502,503,504):
            time.sleep(1.5*(i+1)); continue
        return r
    return r

# ---------- CSV utils (supporto v6 e v7) ----------
def sniff_reader(path):
    with open(path, "r", encoding="utf-8-sig") as f:
        sample = f.read(4096)
        f.seek(0)
        try:
            dialect = csv.Sniffer().sniff(sample, [",",";","\t"])
        except Exception:
            class _D: delimiter=";"
            dialect = _D()
        return csv.DictReader(f, dialect=dialect)

def gf(row, *keys):
    for k in keys:
        if k in row and str(row[k]).strip():
            return str(row[k]).strip()
    return ""

def parse_csv(path):
    rows = []
    rdr = sniff_reader(path)
    for i, r in enumerate(rdr, start=2):
        rows.append((i, r))
    return rows

# ---------- Scraping descrizione + immagini ----------
IMG_EXT = (".jpg",".jpeg",".png",".webp",".gif",".jfif")
MONTHS  = r"(jan(?:uary)?|feb(?:ruary)?|mar(?:ch)?|apr(?:il)?|may|jun(?:e)?|jul(?:y)?|aug(?:ust)?|sep(?:t(?:ember)?)?|oct(?:ober)?|nov(?:ember)?|dec(?:ember)?|gen(?:naio)?|feb(?:braio)?|mar(?:zo)?|apr(?:ile)?|mag(?:gio)?|giu(?:gno)?|lug(?:lio)?|ago(?:sto)?|set(?:tembre)?|ott(?:obre)?|nov(?:embre)?|dic(?:embre)?)"

def abs_urls(base, urls):
    out=[]
    b = "{u.scheme}://{u.netloc}".format(u=urlparse(base))
    for u in urls:
        if not u: continue
        o = urlparse(u)
        out.append(u if o.scheme in ("http","https") else urljoin(b, u))
    return out

def clean_imgs(imgs):
    seen=set(); out=[]
    for u in imgs:
        u0 = u.split("?")[0].lower()
        if not u0.endswith(IMG_EXT): continue
        if any(b in u0 for b in ("thumb","thumbnail","icon","placeholder","/small/","100x100","150x150","lazy")):
            continue
        if u in seen: continue
        seen.add(u); out.append(u)
    return out[:15]

def scrape_page(url):
    try:
        r = requests.get(url, headers={"User-Agent": USER_AGENT}, timeout=30)
        if r.status_code != 200: return None, []
    except:
        return None, []
    soup = BeautifulSoup(r.text, "lxml")

    # Descrizione: cerca blocchi “sotto le foto”
    cands = []
    for sel in [
        ".product-description", "#description", ".description",
        ".product-info__description", ".product-single__description",
        ".tab-content", ".tabs-content", "article", ".desc", ".product__description"
    ]:
        n = soup.select_one(sel)
        if n:
            txt = n.get_text("\n", strip=True)
            if txt and len(txt) > 80:
                cands.append(n)

    if cands:
        # prendo il blocco con più testo
        cands.sort(key=lambda n: len(n.get_text()), reverse=True)
        desc_html = "".join(str(p) for p in cands[0].find_all(["p","li"])[:12]) or str(cands[0])
    else:
        # fallback meta
        meta = soup.find("meta", attrs={"name":"description"}) or soup.find("meta", attrs={"property":"og:description"})
        desc_html = f"<p>{html.escape(meta['content'])}</p>" if meta and meta.get("content") else ""

    # Immagini
    base = url
    imgs=[]
    og = soup.find("meta", attrs={"property":"og:image"})
    if og and og.get("content"): imgs.append(og["content"])
    for gsel in [
        ("div", {"class": re.compile("(gallery|carousel|product[-_ ]images|swiper|slick)", re.I)}),
        ("ul", {"class": re.compile("(thumb|gallery)", re.I)}),
    ]:
        gal = soup.find(*gsel)
        if gal:
            for im in gal.find_all("img"):
                src = im.get("src") or im.get("data-src") or im.get("data-large_image") or im.get("data-zoom-image")
                if src: imgs.append(src)
    for im in soup.find_all("img"):
        src = im.get("src") or im.get("data-src") or im.get("data-original")
        if src: imgs.append(src)

    imgs = clean_imgs(abs_urls(base, imgs))
    return desc_html, imgs

def extract_eta_deadline(text):
    t = " ".join((text or "").split())
    eta = None; deadline = None
    m = re.search(r"\bQ[1-4]\s*-\s*Q[1-4]\s*20\d{2}\b", t, re.I) or re.search(r"\bQ[1-4]\s*20\d{2}\b", t, re.I)
    if m: eta = m.group(0)
    if not eta:
        m = re.search(rf"\b{MONTHS}\s+20\d{{2}}\b", t, re.I)
        if m: eta = m.group(0).title()
    m = re.search(r"(pre[-\s]?order|chiusura\s+preordine|order\s+deadline|bestellschluss)\s*[:\-]?\s*(\d{1,2}[./]\d{1,2}[./]\d{2,4})", t, re.I)
    if m:
        d = m.group(2).replace(".", "/")
        p = d.split("/")
        if len(p[-1])==2: p[-1] = "20"+p[-1]
        deadline = "/".join(p)
    return eta, deadline

# ---------- Media Manager ----------
def media_bulk_import(urls, folder="media-root/preordini"):
    if not urls: return []
    payload = {
        "files": [
            {"url": u, "displayName": os.path.basename(urlparse(u).path) or f"image_{i}.jpg", "filePath": folder}
            for i, u in enumerate(urls)
        ]
    }
    r = http("POST", f"{MEDIA_V1}/files/bulk-import-file", data=json.dumps(payload))
    if r.status_code != 200:
        print(f"[WARN] Bulk Import {r.status_code}: {r.text}")
        return []
    data = r.json()
    out=[]
    for f in data.get("files", []):
        fid = f.get("id")
        if fid: out.append({"fileId": fid, "displayName": f.get("displayName")})
    return out

def product_add_media(product_id, files):
    if not files: return
    body = {"mediaItems": [{"fileId": f["fileId"]} for f in files]}
    r = http("POST", f"{STORES_V1}/products/{product_id}/media", data=json.dumps(body))
    if r.status_code != 200:
        print(f"[WARN] Add media {r.status_code}: {r.text}")

# ---------- Categorie ----------
def categories_by_name():
    r = http("POST", f"{CATEGORIES_V1}/categories/query", data=json.dumps({"query":{}}))
    if r.status_code != 200:
        print(f"[WARN] Categories query {r.status_code}: {r.text}")
        return {}
    by = {}
    for c in r.json().get("categories", []):
        nm = (c.get("name") or "").strip().lower()
        if nm: by[nm] = c.get("id")
    return by

def add_item_to_category(product_id, cat_name):
    if not cat_name: return
    cmap = categories_by_name()
    cid = cmap.get(cat_name.strip().lower())
    if not cid:
        print(f"[WARN] Categoria '{cat_name}' non trovata, salto.")
        return
    body = {"items":[{"catalogItemId": product_id, "appId": WIX_STORES_APP_ID}]}
    r = http("POST", f"{CATEGORIES_V1}/bulk/categories/{cid}/add-items", data=json.dumps(body))
    if r.status_code != 200:
        print(f"[WARN] Add-items {r.status_code}: {r.text}")

# ---------- Creazione prodotto ----------
OPTION_NAME = "PREORDER PAYMENTS OPTIONS*"
CHOICE_DEPOSIT = "ANTICIPO/SALDO"
CHOICE_FULL    = "PAGAMENTO ANTICIPATO"

def make_payload(row):
    # Supporto intestazioni v6 e v7
    name   = gf(row, "nome_articolo", "Nome articolo")
    price  = gf(row, "prezzo_eur", "Prezzo")
    url_d  = gf(row, "url_produttore", "URL pagina distributore")
    sku    = gf(row, "sku", "SKU")
    ean    = gf(row, "gtin_ean", "GTIN/EAN")
    peso   = gf(row, "peso_kg", "Peso (kg)")
    brand  = gf(row, "brand", "Brand", "Brand (seleziona)")
    cat    = gf(row, "categoria", "Categoria", "Categoria (seleziona)")
    descr0 = gf(row, "descrizione", "Descrizione override (opzionale)")
    scad   = gf(row, "preorder_scadenza", "Scadenza preordine (gg/mm/aaaa)")
    eta    = gf(row, "eta", "ETA (mm/aaaa o gg/mm/aaaa)")
    imgs_x = gf(row, "immagini_urls", "URL immagini extra (separate da |) [opzionale]")

    if not name or not price or not url_d:
        raise ValueError("campi minimi mancanti (nome/prezzo/url_distributore)")

    try:
        base_price = round(float(str(price).replace(",", ".")), 2)
    except:
        raise ValueError("prezzo non numerico")

    # scraping
    desc_html, imgs_scraped = scrape_page(url_d)
    # prepend PREORDER line
    header = ""
    if scad or eta:
        parts=[]
        if scad: parts.append(f"Scadenza preordine: {html.escape(scad)}")
        if eta:  parts.append(f"ETA: {html.escape(eta)}")
        header = f"<p><strong>PREORDER</strong> — {' — '.join(parts)}</p>"
    description = (descr0 or desc_html or "")
    if header:
        description = header + (description or "")

    # immagini finali
    extra = [u.strip() for u in imgs_x.split("|")] if imgs_x else []
    image_urls = [u for u in (imgs_scraped + extra) if u]

    # varianti prezzo
    price_deposit = round(base_price*0.30, 2)
    price_fullpay = round(base_price*0.95, 2)

    product = {
        "name": name,
        "productType": "physical",
        "visible": True,
        "description": description[:7900],
        "priceData": {"price": base_price},
        "sku": sku or None,
        "brand": brand or None,
        "weight": float(peso.replace(",", ".")) if peso else None,
        "ribbon": "PREORDER",
        "manageVariants": True,
        "productOptions": [{
            "name": OPTION_NAME,
            "choices": [
                {"value": CHOICE_DEPOSIT, "description": CHOICE_DEPOSIT},
                {"value": CHOICE_FULL,    "description": CHOICE_FULL}
            ]
        }],
        "variants": [
            {"choices": {OPTION_NAME: CHOICE_DEPOSIT}, "priceData": {"price": price_deposit}, "sku": f"{sku}-AS" if sku else None},
            {"choices": {OPTION_NAME: CHOICE_FULL},    "priceData": {"price": price_fullpay}, "sku": f"{sku}-PA" if sku else None}
        ]
    }
    return product, image_urls, cat

def create_product(product):
    body = {"product": product}  # <-- wrapper obbligatorio per V1
    r = http("POST", f"{STORES_V1}/products", data=json.dumps(body))
    return r

def run(csv_path):
    # precheck
    t = http("POST", f"{STORES_V1}/products/query", data=json.dumps({"query":{"paging":{"limit":1}}}))
    if t.status_code != 200:
        fail(f"[ERRORE] API non valide: {t.status_code} {t.text}")

    rows = parse_csv(csv_path)
    created = 0
    for line, row in rows:
        try:
            product, img_urls, categoria = make_payload(row)
        except Exception as e:
            print(f"[ERRORE] Riga {line}: {e}")
            continue

        r = create_product(product)
        if r.status_code != 200:
            print(f"[ERRORE] Riga {line} '{product.get('name','')[:60]}': POST /products {r.status_code}: {r.text}")
            continue

        pid = (r.json().get("product") or {}).get("id")
        if not pid:
            print(f"[ERRORE] Riga {line}: prodotto creato ma senza id.")
            continue

        # immagini: import + attach
        try:
            if img_urls:
                files = media_bulk_import(img_urls)
                if files:
                    product_add_media(pid, files)
                else:
                    print(f"[WARN] Riga {line}: nessuna immagine importata.")
        except Exception as e:
            print(f"[WARN] Immagini: {e}")

        # categoria
        try:
            if categoria:
                add_item_to_category(pid, categoria)
        except Exception as e:
            print(f"[WARN] Categoria: {e}")

        print(f"[OK] Riga {line} creato '{product['name']}' | Varianti: AS={product['variants'][0]['priceData']['price']} PA={product['variants'][1]['priceData']['price']}")
        created += 1

    if created == 0:
        fail("[ERRORE] Nessun prodotto creato.", 2)
    print(f"[FINE] Prodotti creati: {created}")

if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--csv", required=True)
    args = ap.parse_args()
    if not os.path.exists(args.csv):
        fail(f"File non trovato: {args.csv}")
    run(args.csv)
