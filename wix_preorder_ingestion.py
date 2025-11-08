#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os, sys, csv, time, re, html, json, io, zipfile, mimetypes
import argparse
import requests
from urllib.parse import urljoin, urlparse
from bs4 import BeautifulSoup

# ===== Env =====
WIX_API_KEY  = os.getenv("WIX_API_KEY", "").strip()
WIX_SITE_ID  = os.getenv("WIX_SITE_ID", "").strip()   # metaSiteId (quello che ti ha dato 200)
USER_AGENT   = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome Safari"

# ===== Endpoints =====
STORES_V1       = "https://www.wixapis.com/stores/v1"
CATEGORIES_V1   = "https://www.wixapis.com/categories/v1"
COLLECTIONS_V1  = "https://www.wixapis.com/stores/v1/collections"
MEDIA_V1        = "https://www.wixapis.com/site-media/v1"
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
    print(msg, file=sys.stderr); sys.exit(code)

def http(method, url, **kw):
    for i in range(3):
        r = session.request(method, url, timeout=50, **kw)
        if r.status_code in (429,500,502,503,504):
            time.sleep(1.2*(i+1)); continue
        return r
    return r

# ---------- CSV (v6/v7) ----------
def detect_rows(path):
    with open(path, "r", encoding="utf-8-sig", newline="") as f:
        sample = f.read(4096); f.seek(0)
        try:
            dialect = csv.Sniffer().sniff(sample, [",",";","\t"])
        except Exception:
            class _D: delimiter=";"; pass
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
        if u in seen: continue
        seen.add(u); out.append(u)
    return out[:30]

def pick_desc_block(soup):
    selectors = [
        "div.product-description","div.product__description","div.product-info__description",
        "#product-description","#description",".description",
        ".tab-content .active",".tabs-content","article"
    ]
    for sel in selectors:
        n = soup.select_one(sel)
        if n:
            ps = [p for p in n.find_all(["p","li","div"]) if p.get_text(strip=True)]
            txt = " ".join(p.get_text(" ", strip=True) for p in ps)
            if len(txt) > 120:
                return "".join(str(p) for p in ps[:20])
    m = soup.find("meta", attrs={"name":"description"}) or soup.find("meta", attrs={"property":"og:description"})
    if m and m.get("content"): return f"<p>{html.escape(m['content'])}</p>"
    return ""

ETA_PATTERNS = [
    r"\bETA\s*:\s*([A-Z]+(?:\s*\d{1,2}/\d{4})?|[A-Z]+\s*\d{4}|Q[1-4](?:\s*-\s*Q[1-4])?\s*20\d{2})",
    r"\bETA\s*([A-Z]+(?:\s*\d{1,2}/\d{4})?|[A-Z]+\s*\d{4}|Q[1-4](?:\s*-\s*Q[1-4])?\s*20\d{2})"
]
DEAD_PATTERNS = [
    r"\bDeadline\s*:\s*(\d{1,2}[./]\d{1,2}[./]\d{2,4})",
    r"\bOrder\s*deadline\s*[:\-]?\s*(\d{1,2}[./]\d{1,2}[./]\d{2,4})",
    r"\bChiusura\s*preordine\s*[:\-]?\s*(\d{1,2}[./]\d{1,2}[./]\d{2,4})"
]

def extract_eta_deadline(text):
    t = " ".join((text or "").split())
    eta = None; dead = None
    for pat in ETA_PATTERNS:
        m = re.search(pat, t, re.I)
        if m: eta = m.group(1).strip(); break
    for pat in DEAD_PATTERNS:
        m = re.search(pat, t, re.I)
        if m:
            d = m.group(1).replace(".", "/")
            p = d.split("/")
            if len(p[-1]) == 2: p[-1] = "20"+p[-1]
            dead = "/".join(p)
            break
    return eta, dead

def find_zip_link(url, soup):
    # prova testo visibile
    for a in soup.find_all("a", href=True):
        txt = (a.get_text(" ", strip=True) or "").lower()
        if any(k in txt for k in ["scarica immagini","download images","bilder herunterladen"]):
            return abs_urls(url, [a["href"]])[0]
    # prova title/aria-label
    for a in soup.select("a[href][title], a[href][aria-label]"):
        t = ((a.get("title") or "") + " " + (a.get("aria-label") or "")).lower()
        if any(k in t for k in ["scarica immagini","download images","bilder herunterladen"]):
            return abs_urls(url, [a["href"]])[0]
    return None

def scrape_page(url):
    try:
        r = requests.get(url, headers={"User-Agent": USER_AGENT}, timeout=40)
        if r.status_code != 200:
            return {"desc":"", "images":[], "zip_url":None, "full_text":""}
    except:
        return {"desc":"", "images":[], "zip_url":None, "full_text":""}

    soup = BeautifulSoup(r.text, "lxml")
    desc_html = pick_desc_block(soup)

    zip_url = find_zip_link(url, soup)

    # immagini: <a href=*.jpg>, <img>, <source srcset>
    img_urls = []
    for a in soup.select("a[href]"):
        href = a.get("href","")
        if any(href.lower().split("?")[0].endswith(ext) for ext in IMG_EXT):
            img_urls.append(href)

    for im in soup.find_all("img"):
        src = im.get("data-zoom-image") or im.get("data-large_image") or im.get("data-src") or im.get("src")
        if src: img_urls.append(src)

    for src in soup.select("source[srcset]"):
        ss = src.get("srcset") or ""
        for part in ss.split(","):
            u = part.strip().split(" ")[0]
            if u: img_urls.append(u)

    img_urls = clean_imgs(abs_urls(url, img_urls))
    full_text = soup.get_text(" ", strip=True)
    return {"desc": desc_html, "images": img_urls, "zip_url": zip_url, "full_text": full_text}

# ---------- Media ----------
def media_bulk_import(urls, folder="media-root/preordini"):
    if not urls: return []
    payload = {"files":[{"url":u,"displayName": os.path.basename(urlparse(u).path) or f"image_{i}.jpg","filePath":folder} for i,u in enumerate(urls)]}
    r = http("POST", f"{MEDIA_V1}/files/bulk-import-file", data=json.dumps(payload))
    if r.status_code != 200:
        print(f"[WARN] Bulk Import {r.status_code}: {r.text[:200]}")
        return []
    data = r.json(); out=[]
    for f in data.get("files", []):
        fid = f.get("id")
        if fid: out.append({"fileId": fid, "displayName": f.get("displayName")})
    return out

def media_upload_bytes(name, content, folder="media-root/preordini"):
    headers = {k:v for k,v in session.headers.items() if k.lower()!="content-type"}
    files = {"file": (name, content, mimetypes.guess_type(name)[0] or "application/octet-stream")}
    data = {"filePath": folder, "displayName": name}
    r = requests.post(f"{MEDIA_V1}/files/upload", headers=headers, files=files, data=data, timeout=120)
    if r.status_code != 200:
        print(f"[WARN] Upload {name} -> {r.status_code}: {r.text[:200]}")
        return None
    fid = (r.json().get("file") or {}).get("id")
    return {"fileId": fid, "displayName": name} if fid else None

def product_add_media(product_id, files):
    if not files: return
    body = {"mediaItems": [{"fileId": f["fileId"]} for f in files if f and f.get("fileId")]}
    r = http("POST", f"{STORES_V1}/products/{product_id}/media", data=json.dumps(body))
    if r.status_code != 200:
        print(f"[WARN] Add media {r.status_code}: {r.text[:200]}")

def import_images(scraped):
    out=[]
    if scraped.get("zip_url"):
        try:
            z = requests.get(scraped["zip_url"], headers={"User-Agent": USER_AGENT}, timeout=120)
            z.raise_for_status()
            with zipfile.ZipFile(io.BytesIO(z.content)) as zf:
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

    urls = scraped.get("images") or []
    if urls:
        out = media_bulk_import(urls)
        if out:
            print(f"[INFO] Import URL: {len(out)} immagini caricate")
    return out

# ---------- Categorie / Collections ----------
def categories_by_name():
    r = http("POST", f"{CATEGORIES_V1}/categories/query", data=json.dumps({"query":{}}))
    if r.status_code != 200: return {}
    return { (c.get("name") or "").strip().lower(): c.get("id") for c in r.json().get("categories", []) }

def collections_by_name():
    r = http("POST", f"{COLLECTIONS_V1}/query", data=json.dumps({"query":{}}))
    if r.status_code != 200: return {}
    return { (c.get("name") or "").strip().lower(): c.get("id") for c in r.json().get("collections", []) }

def add_to_category_or_collection(product_id, name):
    if not name: return
    nm = name.strip().lower()

    cmap = categories_by_name()
    if nm in cmap:
        body = {"items":[{"catalogItemId": product_id, "appId": WIX_STORES_APP_ID}]}
        r = http("POST", f"{CATEGORIES_V1}/bulk/categories/{cmap[nm]}/add-items", data=json.dumps(body))
        if r.status_code == 200:
            print(f"[INFO] Assegnato a categoria '{name}'"); return
        print(f"[WARN] Categories add-items {r.status_code}: {r.text[:200]}")

    colmap = collections_by_name()
    if nm in colmap:
        r = http("POST", f"{COLLECTIONS_V1}/{colmap[nm]}/productIds", data=json.dumps({"productIds":[product_id]}))
        if r.status_code == 200:
            print(f"[INFO] Assegnato a collection '{name}'"); return
        print(f"[WARN] Collections add {r.status_code}: {r.text[:200]}")

    print(f"[WARN] Nessuna categoria/collection trovata per '{name}'")

# ---------- Product ----------
OPTION_NAME = "PREORDER PAYMENTS OPTIONS*"
CHOICE_DEPOSIT = "ANTICIPO/SALDO"
CHOICE_FULL    = "PAGAMENTO ANTICIPATO"

def make_payload(row):
    # v6/v7 compat
    name   = gf(row,"nome_articolo","Nome articolo")
    price  = gf(row,"prezzo_eur","Prezzo")
    url_d  = gf(row,"url_produttore","URL pagina distributore")
    sku    = gf(row,"sku","SKU")
    peso   = gf(row,"peso_kg","Peso (kg)")
    brand  = gf(row,"brand","Brand","Brand (seleziona)")
    cat    = gf(row,"categoria","Categoria","Categoria (seleziona)")
    descr0 = gf(row,"descrizione","Descrizione override (opzionale)")
    scad   = gf(row,"preorder_scadenza","Scadenza preordine (gg/mm/aaaa)")
    eta    = gf(row,"eta","ETA (mm/aaaa o gg/mm/aaaa)")
    imgs_x = gf(row,"immagini_urls","URL immagini extra (separate da |) [opzionale]")

    if not name or not price or not url_d:
        raise ValueError("mancano nome/prezzo/url_distributore")

    base_price = round(float(str(price).replace(",", ".")), 2)

    # scrape pagina
    scraped = scrape_page(url_d)
    e2, d2 = extract_eta_deadline(scraped.get("full_text",""))
    eta = eta or e2
    scad = scad or d2

    header = ""
    if eta:  header += f"<p><strong>Uscita prevista: {html.escape(eta)}</strong></p>"
    if scad: header += f"<p><strong>Chiusura preordine : {html.escape(scad)} Salvo esaurimento</strong></p>"

    desc_html = (descr0 or scraped.get("desc") or "")
    description = (header + (desc_html or ""))[:7900]

    # immagini finali
    extra = [u.strip() for u in imgs_x.split("|")] if imgs_x else []
    scraped["images"] = (scraped.get("images") or []) + extra

    # varianti (forzo sia priceData.price che price)
    p_deposit = round(base_price * 0.30, 2)
    p_full    = round(base_price * 0.95, 2)

    product = {
        "name": name,
        "productType": "physical",
        "visible": True,
        "description": description,
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
            {"choices": {OPTION_NAME: CHOICE_DEPOSIT}, "price": p_deposit, "priceData": {"price": p_deposit}, "sku": f"{sku}-AS" if sku else None},
            {"choices": {OPTION_NAME: CHOICE_FULL},    "price": p_full,    "priceData": {"price": p_full},    "sku": f"{sku}-PA" if sku else None}
        ]
    }
    return product, scraped, cat

def create_product(product):
    body = {"product": product}
    r = http("POST", f"{STORES_V1}/products", data=json.dumps(body))
    return r

def get_product(pid):
    r = http("GET", f"{STORES_V1}/products/{pid}")
    try:
        return r.json().get("product", {})
    except Exception:
        return {}

def patch_variants(pid, variants):
    body = {"product": {"id": pid, "variants": variants}}
    r = http("PATCH", f"{STORES_V1}/products/{pid}", data=json.dumps(body))
    return r

def run(csv_path):
    # precheck
    t = http("POST", f"{STORES_V1}/products/query", data=json.dumps({"query":{"paging":{"limit":1}}}))
    if t.status_code != 200:
        fail(f"[ERRORE] API non valide: {t.status_code} {t.text}")

    rows = detect_rows(csv_path)
    created = 0
    for line, row in rows:
        name = gf(row,"nome_articolo","Nome articolo") or "(senza nome)"
        try:
            product, scraped, categoria = make_payload(row)
        except Exception as e:
            print(f"[ERRORE] Riga {line}: {e}"); continue

        r = create_product(product)
        if r.status_code != 200:
            print(f"[ERRORE] Riga {line} '{name[:60]}': POST /products {r.status_code}: {r.text[:300]}"); continue

        pid = (r.json().get("product") or {}).get("id")
        if not pid:
            print(f"[ERRORE] Riga {line}: prodotto creato ma senza id."); continue

        # immagini (zip/url) + attach
        try:
            files = import_images(scraped)
            if files: product_add_media(pid, files)
            else:     print(f"[WARN] Riga {line} '{name}': nessuna immagine caricata.")
        except Exception as e:
            print(f"[WARN] Immagini: {e}")

        # categoria / collection
        try:
            if categoria: add_to_category_or_collection(pid, categoria)
        except Exception as e:
            print(f"[WARN] Categoria/Collection: {e}")

        # post-check varianti lette da Wix
        stored = get_product(pid)
        try:
            sv = stored.get("variants", []) or []
            sv_prices = [(v.get('choices'), v.get('priceData',{}).get('price'), v.get('price')) for v in sv]
            print(f"[CHECK] Varianti salvate da Wix: {sv_prices}")
            # se Wix ha messo il prezzo base, forzo un PATCH con i prezzi giusti
            base = product["priceData"]["price"]
            want = {
                CHOICE_DEPOSIT: round(base*0.30,2),
                CHOICE_FULL:    round(base*0.95,2)
            }
            needs_patch = False
            patched = []
            for v in sv:
                choice_val = list((v.get("choices") or {}).values())[0]
                target = want.get(choice_val)
                if target is None:
                    patched.append(v); continue
                pv = v.get("priceData",{}).get("price")
                p  = v.get("price")
                if pv != target or p != target:
                    v["priceData"] = {"price": target}
                    v["price"] = target
                    needs_patch = True
                patched.append(v)
            if needs_patch:
                pr = patch_variants(pid, patched)
                print(f"[CHECK] Patch varianti -> {pr.status_code}")
        except Exception as e:
            print(f"[WARN] Post-check varianti: {e}")

        print(f"[OK] Riga {line} creato '{name}'")
        created += 1

    if created == 0:
        fail("[ERRORE] Nessun prodotto creato.", 2)
    print(f"[FINE] Prodotti creati: {created}")

if __name__ == "__main__":
    ap = argparse.ArgumentParser(); ap.add_argument("--csv", required=True)
    args = ap.parse_args()
    if not os.path.exists(args.csv): fail(f"File non trovato: {args.csv}")
    run(args.csv)
