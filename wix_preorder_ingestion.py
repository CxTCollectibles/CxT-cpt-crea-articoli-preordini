#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os, sys, csv, time, re, html, json, io, zipfile, mimetypes
import argparse
import requests
from urllib.parse import urljoin, urlparse
from bs4 import BeautifulSoup

# --- Config da env ---
WIX_API_KEY  = os.getenv("WIX_API_KEY", "").strip()
WIX_SITE_ID  = os.getenv("WIX_SITE_ID", "").strip()  # metaSiteId (quello che ti risponde 200)
USER_AGENT   = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome Safari"

# --- Endpoint ---
STORES_V1      = "https://www.wixapis.com/stores/v1"
COLLECTIONS_V1 = "https://www.wixapis.com/stores/v1/collections"
CATEGORIES_V1  = "https://www.wixapis.com/categories/v1"
MEDIA_V1       = "https://www.wixapis.com/site-media/v1"
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
        r = session.request(method, url, timeout=45, **kw)
        if r.status_code in (429,500,502,503,504):
            time.sleep(1.2*(i+1)); continue
        return r
    return r

# ---------- CSV v6/v7 ----------
def detect_rows(path):
    with open(path, "r", encoding="utf-8-sig", newline="") as f:
        sample = f.read(4096); f.seek(0)
        try:
            dialect = csv.Sniffer().sniff(sample, [",",";","\t"])
        except Exception:
            class _D: delimiter=";"
            dialect = _D()
        rdr = csv.DictReader(f, dialect=dialect)
        return [(i, r) for i, r in enumerate(rdr, start=2)]

def gf(row, *keys):
    for k in keys:
        if k in row and str(row[k]).strip():
            return str(row[k]).strip()
    return ""

# ---------- Scraping ----------
IMG_EXT = (".jpg",".jpeg",".png",".webp",".gif",".jfif")

def abs_urls(base, urls):
    base_root = "{u.scheme}://{u.netloc}".format(u=urlparse(base))
    out=[]
    for u in urls:
        if not u: continue
        o = urlparse(u)
        out.append(u if o.scheme in ("http","https") else urljoin(base_root, u))
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
    return out[:20]

def pick_desc_block(soup):
    # selettori “centrali” tipici (come nello screenshot: descrizione sotto il titolo)
    selectors = [
        "#product-description", ".product-description", ".product__description", ".product-info__description",
        "#description", ".description", ".tab-content", ".tabs-content", "article .content", "article"
    ]
    for sel in selectors:
        n = soup.select_one(sel)
        if n:
            ps = [p for p in n.find_all(["p","li"]) if p.get_text(strip=True)]
            if ps:
                txt = " ".join(p.get_text(" ", strip=True) for p in ps)
                if len(txt) > 120:
                    return "".join(str(p) for p in ps[:18])
            txt = n.get_text(" ", strip=True)
            if len(txt) > 120:
                return str(n)
    # meta fallback
    m = soup.find("meta", attrs={"name":"description"}) or soup.find("meta", attrs={"property":"og:description"})
    if m and m.get("content"):
        return f"<p>{html.escape(m['content'])}</p>"
    return ""

def domain_handler(url, soup):
    """handler specifici per alcuni distributori (es. heo)"""
    host = urlparse(url).netloc.lower()
    imgs = []
    # heo.*: pulsante "Scarica immagini"/"Download images" -> zip
    if "heo." in host:
        # 1) prova “scarica immagini” (italiano/inglese/tedesco)
        link = None
        for a in soup.find_all("a", href=True):
            txt = (a.get_text(" ", strip=True) or "").lower()
            if any(k in txt for k in ["scarica immagini","download images","bilder herunterladen"]):
                link = a["href"]; break
        if link:
            return "ZIP", abs_urls(url, [link])[0]
        # 2) gallery classica
        for im in soup.select("div.gallery img, .swiper img, .slick img, img"):
            src = im.get("data-zoom-image") or im.get("data-large_image") or im.get("data-src") or im.get("src")
            if src: imgs.append(src)
        return "HTML", clean_imgs(abs_urls(url, imgs))

    # sideshow.com: spesso ha JSON-LD con array image
    if "sideshow" in host:
        for tag in soup.find_all("script", type="application/ld+json"):
            try:
                data = json.loads(tag.string or "")
                if isinstance(data, dict) and data.get("@type") == "Product":
                    arr = data.get("image")
                    if isinstance(arr, list):
                        return "HTML", [u for u in arr if isinstance(u,str)]
            except Exception:
                pass
    return "HTML", None  # default

def scrape_page(url):
    try:
        r = requests.get(url, headers={"User-Agent": USER_AGENT}, timeout=35)
        if r.status_code != 200: return {"desc":"", "images":[]}
    except:
        return {"desc":"", "images":[]}

    soup = BeautifulSoup(r.text, "lxml")
    # descrizione
    desc_html = pick_desc_block(soup)

    # immagini
    mode, special = domain_handler(url, soup)
    images=[]
    if mode == "ZIP" and isinstance(special, str):
        # segnalo allo strato upload che c'è uno zip da scaricare
        return {"desc":desc_html, "images":[], "zip_url": special}
    if mode == "HTML" and isinstance(special, list):
        images = special
    if not images:
        # generico: og:image + gallery + tutte le <img>
        cand=[]
        og = soup.find("meta", attrs={"property":"og:image"})
        if og and og.get("content"): cand.append(og["content"])
        for im in soup.select("div.gallery img, .swiper img, .slick img, img"):
            src = im.get("data-zoom-image") or im.get("data-large_image") or im.get("data-src") or im.get("src")
            if src: cand.append(src)
        images = clean_imgs(abs_urls(url, cand))
    return {"desc":desc_html, "images":images}

# ---------- Media Manager ----------
def media_bulk_import(urls, folder="media-root/preordini"):
    if not urls: return []
    payload = {"files":[{"url":u, "displayName": os.path.basename(urlparse(u).path) or f"image_{i}.jpg","filePath":folder} for i,u in enumerate(urls)]}
    r = http("POST", f"{MEDIA_V1}/files/bulk-import-file", data=json.dumps(payload))
    if r.status_code != 200:
        print(f"[WARN] Bulk Import {r.status_code}: {r.text}")
        return []
    data = r.json(); out=[]
    for f in data.get("files", []):
        fid = f.get("id")
        if fid: out.append({"fileId": fid, "displayName": f.get("displayName")})
    return out

def media_upload_bytes(name, content, folder="media-root/preordini"):
    files = {"file": (name, content, mimetypes.guess_type(name)[0] or "application/octet-stream")}
    data = {"filePath": folder, "displayName": name}
    # per upload multipart bisogna togliere Content-Type json a livello di sessione
    headers = {k:v for k,v in session.headers.items() if k.lower()!="content-type"}
    r = requests.post(f"{MEDIA_V1}/files/upload", headers=headers, files=files, data=data, timeout=60)
    if r.status_code != 200:
        print(f"[WARN] Upload file {name} -> {r.status_code}: {r.text}")
        return None
    fid = (r.json().get("file") or {}).get("id")
    return {"fileId": fid, "displayName": name} if fid else None

def product_add_media(product_id, files):
    if not files: return
    body = {"mediaItems": [{"fileId": f["fileId"]} for f in files if f and f.get("fileId")]}
    r = http("POST", f"{STORES_V1}/products/{product_id}/media", data=json.dumps(body))
    if r.status_code != 200:
        print(f"[WARN] Add media {r.status_code}: {r.text}")

def import_images(url, scraped):
    """ritorna lista di dict {fileId,...} caricati su Wix"""
    out=[]
    # priorità: zip (Scarica immagini)
    if scraped.get("zip_url"):
        try:
            z = requests.get(scraped["zip_url"], headers={"User-Agent": USER_AGENT}, timeout=60)
            z.raise_for_status()
            with zipfile.ZipFile(io.BytesIO(z.content)) as zf:
                # prendo i file immagine
                names = [n for n in zf.namelist() if os.path.splitext(n.lower())[1] in IMG_EXT]
                for n in names:
                    with zf.open(n) as fh:
                        data = fh.read()
                        up = media_upload_bytes(os.path.basename(n), data)
                        if up: out.append(up)
            if out:
                print(f"[INFO] Import ZIP: {len(out)} immagini caricate")
                return out
        except Exception as e:
            print(f"[WARN] ZIP import fallito: {e}")
    # fallback: url diretti
    imgs = scraped.get("images") or []
    if imgs:
        out = media_bulk_import(imgs)
        if out:
            print(f"[INFO] Import URL: {len(out)} immagini caricate")
    return out

# ---------- Categorie / Collezioni ----------
def categories_by_name():
    r = http("POST", f"{CATEGORIES_V1}/categories/query", data=json.dumps({"query":{}}))
    if r.status_code != 200:
        return {}
    by = {}
    for c in r.json().get("categories", []):
        nm = (c.get("name") or "").strip().lower()
        if nm: by[nm] = c.get("id")
    return by

def collections_by_name():
    r = http("POST", f"{COLLECTIONS_V1}/query", data=json.dumps({"query":{}}))
    if r.status_code != 200:
        return {}
    by={}
    for c in r.json().get("collections", []):
        nm = (c.get("name") or "").strip().lower()
        if nm: by[nm] = c.get("id")
    return by

def add_to_category_or_collection(product_id, name):
    if not name: return
    nm = name.strip().lower()
    # 1) categorie
    cmap = categories_by_name()
    cid = cmap.get(nm)
    if cid:
        body = {"items":[{"catalogItemId": product_id, "appId": WIX_STORES_APP_ID}]}
        r = http("POST", f"{CATEGORIES_V1}/bulk/categories/{cid}/add-items", data=json.dumps(body))
        if r.status_code == 200:
            print(f"[INFO] Assegnato a categoria '{name}'")
            return
        print(f"[WARN] Categories add-items {r.status_code}: {r.text}")
    # 2) collections (fallback)
    coll = collections_by_name()
    lid = coll.get(nm)
    if lid:
        r = http("POST", f"{COLLECTIONS_V1}/{lid}/productIds", data=json.dumps({"productIds":[product_id]}))
        if r.status_code == 200:
            print(f"[INFO] Assegnato a collection '{name}'")
            return
        print(f"[WARN] Collections add {r.status_code}: {r.text}")
    print(f"[WARN] Nessuna categoria/collection trovata per '{name}'")

# ---------- Prodotto ----------
OPTION_NAME = "PREORDER PAYMENTS OPTIONS*"
CHOICE_DEPOSIT = "ANTICIPO/SALDO"
CHOICE_FULL    = "PAGAMENTO ANTICIPATO"

def make_payload(row):
    # v6/v7 compat
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
        raise ValueError("mancano nome/prezzo/url_distributore")

    base_price = round(float(str(price).replace(",", ".")), 2)

    # scraping pagina
    scraped = scrape_page(url_d)
    desc_html = scraped.get("desc") or ""
    # prepend PREORDER in testa se ho dati
    header = ""
    if scad or eta:
        parts=[]
        if scad: parts.append(f"Scadenza preordine: {html.escape(scad)}")
        if eta:  parts.append(f"ETA: {html.escape(eta)}")
        header = f"<p><strong>PREORDER</strong> — {' — '.join(parts)}</p>"
    description = (descr0 or desc_html or "")
    if header: description = header + (description or "")

    # immagini: zip/images + extra dal csv
    extra = [u.strip() for u in imgs_x.split("|")] if imgs_x else []
    scraped["images"] = (scraped.get("images") or []) + extra

    # varianti: doppio campo (priceData + price) per forzare Wix
    price_deposit = round(base_price*0.30, 2)
    price_fullpay = round(base_price*0.95, 2)

    product = {
        "name": name,
        "productType": "physical",
        "visible": True,
        "description": (description or "")[:7900],
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
            {"choices": {OPTION_NAME: CHOICE_DEPOSIT}, "priceData": {"price": price_deposit}, "price": price_deposit, "sku": f"{sku}-AS" if sku else None},
            {"choices": {OPTION_NAME: CHOICE_FULL},    "priceData": {"price": price_fullpay}, "price": price_fullpay, "sku": f"{sku}-PA" if sku else None}
        ]
    }
    return product, scraped, cat

def create_product(product):
    body = {"product": product}
    r = http("POST", f"{STORES_V1}/products", data=json.dumps(body))
    return r

def run(csv_path):
    # precheck
    t = http("POST", f"{STORES_V1}/products/query", data=json.dumps({"query":{"paging":{"limit":1}}}))
    if t.status_code != 200:
        fail(f"[ERRORE] API non valide: {t.status_code} {t.text}")

    rows = detect_rows(csv_path)
    created = 0
    for line, row in rows:
        name = gf(row, "nome_articolo","Nome articolo") or "(senza nome)"
        try:
            product, scraped, categoria = make_payload(row)
        except Exception as e:
            print(f"[ERRORE] Riga {line}: {e}")
            continue

        r = create_product(product)
        if r.status_code != 200:
            print(f"[ERRORE] Riga {line} '{name[:60]}': POST /products {r.status_code}: {r.text}")
            continue

        pid = (r.json().get("product") or {}).get("id")
        if not pid:
            print(f"[ERRORE] Riga {line}: prodotto creato ma senza id."); continue

        # immagini: zip/import + attach
        try:
            files = import_images(gf(row,"url_produttore","URL pagina distributore"), scraped)
            if files:
                product_add_media(pid, files)
            else:
                print(f"[WARN] Riga {line} '{name}': nessuna immagine caricata")
        except Exception as e:
            print(f"[WARN] Immagini: {e}")

        # categoria o collection
        try:
            if categoria:
                add_to_category_or_collection(pid, categoria)
        except Exception as e:
            print(f"[WARN] Categoria/Collection: {e}")

        # conferma varianti
        v1 = product["variants"][0]["priceData"]["price"]
        v2 = product["variants"][1]["priceData"]["price"]
        print(f"[OK] Riga {line} creato '{name}' | Var: AS={v1}  PA={v2}")
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
