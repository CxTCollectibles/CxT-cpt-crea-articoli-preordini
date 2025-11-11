#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import argparse, csv, os, sys, time, json, re, unicodedata
from urllib.parse import urlparse
import requests
from bs4 import BeautifulSoup
from slugify import slugify
from langdetect import detect, DetectorFactory
from deep_translator import MyMemoryTranslator

DetectorFactory.seed = 0

API_KEY = os.environ.get("WIX_API_KEY", "").strip()
SITE_ID = os.environ.get("WIX_SITE_ID", "").strip()

BASE_V1 = "https://www.wixapis.com/stores/v1"
CURRENCY = "EUR"
TIMEOUT = 30

# ---------------- HTTP ----------------
def _headers():
    if not API_KEY or not SITE_ID:
        print("[ERRORE] Mancano WIX_API_KEY o WIX_SITE_ID nei secrets.", file=sys.stderr)
        sys.exit(2)
    return {
        "Authorization": API_KEY,
        "wix-site-id": SITE_ID,
        "Content-Type": "application/json",
    }

def http(method, url, payload=None):
    data_str = json.dumps(payload) if payload is not None else None
    r = requests.request(method, url, timeout=TIMEOUT, headers=_headers(), data=data_str)
    if r.status_code >= 400:
        raise RuntimeError(f"{method} {url} failed {r.status_code}: {r.text}")
    try:
        return r.json() if r.text else {}
    except Exception:
        return {"_raw": r.text}

# ---------------- UTIL ----------------
def norm(s):
    if s is None: return ""
    s = unicodedata.normalize("NFKD", s)
    s = "".join(ch for ch in s if not unicodedata.combining(ch))
    return s.strip().casefold()

def money(x):
    try: v = float(str(x).replace(",", "."))
    except: v = 0.0
    return round(max(0.0, v), 2)

def limit_len(s, n):
    return s if len(s) <= n else s[:n]

def to_html(p):
    return "<div><p>" + p.replace("\r","").replace("\n","<br>") + "</p></div>"

# ---------------- CSV ----------------
def parse_csv(path):
    with open(path, "r", encoding="utf-8-sig", newline="") as f:
        sample = f.read(8192)
    delimiter = ";" if sample.count(";") >= sample.count(",") else ","
    rows = []
    with open(path, "r", encoding="utf-8-sig", newline="") as f:
        rdr = csv.DictReader(f, delimiter=delimiter)
        if rdr.fieldnames is None:
            raise RuntimeError("CSV senza header")
        for i, r in enumerate(rdr, start=2):
            row = { (k or "").strip().lower(): (v or "").strip() for k,v in r.items() }
            if not row.get("nome_articolo") or not row.get("prezzo_eur"):
                print(f"[SKIP] Riga {i}: nome_articolo o prezzo_eur mancante.")
                continue
            rows.append(row)
    if not rows:
        raise RuntimeError("CSV vuoto o non valido.")
    print(f"[INFO] CSV encoding=utf-8-sig delimiter='{delimiter}'")
    return rows

# ---------------- ETA/Deadline ----------------
def quarter_from_date_or_text(t):
    if not t: return None
    t = t.strip()
    # mm/yyyy
    m = re.search(r"(\d{1,2})/(\d{4})", t)
    month = year = None
    if m:
        month = int(m.group(1)); year = int(m.group(2))
    else:
        MMM = {"jan":1,"feb":2,"mar":3,"apr":4,"may":5,"jun":6,"jul":7,"aug":8,"sep":9,"oct":10,"nov":11,"dec":12}
        m2 = re.search(r"([A-Za-z]{3,})\s+(\d{4})", t)
        if m2:
            month = MMM.get(m2.group(1).lower()[:3]); year = int(m2.group(2))
    if not (month and year): return None
    q = (month-1)//3 + 1
    qr = f"Q{q}"
    if "FINE" in t.upper() or month in (3,6,9,12):
        qr = f"Q{q} - Q{min(4,q+1)}" if q < 4 else "Q4"
    return qr

def build_banner(eta_text, deadline_text):
    parts = []
    q = quarter_from_date_or_text(eta_text) if eta_text else None
    if q: parts.append(f"Uscita prevista: {q}")
    if deadline_text: parts.append(f"Deadline preordine: {deadline_text}")
    if not parts: return ""
    return "**" + " | ".join(parts) + "**\n\n"

# ---------------- SCRAPING ----------------
SKU_PAT = r"(Codice Prodotto|Codice articolo|Artikelnummer|SKU)\s*[:#]?\s*([A-Z0-9\-_.]{4,})"
GTIN_PAT = r"(GTIN|EAN|UPC)\s*[:#]?\s*(\d{8,14})"
DDL_PAT  = r"(Deadline|Scadenza|Order Deadline|Preorder deadline)\s*[:#]?\s*([0-3]?\d\.[01]?\d\.\d{4})"
ETA_PAT  = r"(ETA|Uscita prevista|Release|Disponibile da)\s*[:#]?\s*([A-Za-z]{3,}\s*\d{4}|\d{2}/\d{4}|FINE\s*\d{2}/\d{4})"

def scrape_all(url):
    out = {"description":"", "eta":None, "deadline":None, "sku":None, "gtin":None}
    if not url: return out
    try:
        h = requests.get(url, timeout=30, headers={"User-Agent":"Mozilla/5.0"}).text
        soup = BeautifulSoup(h, "lxml")

        # JSON-LD first
        for tag in soup.find_all("script", {"type":"application/ld+json"}):
            try:
                j = json.loads(tag.string or "")
            except Exception:
                continue
            def pick(obj):
                if isinstance(obj, list):
                    for x in obj: pick(x); return
                if not isinstance(obj, dict): return
                if str(obj.get("@type","")).lower().endswith("product"):
                    out["description"] = out["description"] or obj.get("description","")
                    out["sku"] = out["sku"] or obj.get("sku")
                    out["gtin"] = out["gtin"] or obj.get("gtin13") or obj.get("gtin") or obj.get("gtin14") or obj.get("gtin8")
            pick(j)

        # fallback desc: og:description
        if not out["description"]:
            meta = soup.select_one('meta[property="og:description"]') or soup.select_one('meta[name="description"]')
            if meta and meta.get("content"): out["description"] = meta["content"]

        # if still short, collect longest paragraph
        if not out["description"] or len(out["description"]) < 200:
            txts = [t.get_text(" ", strip=True) for t in soup.select("main, #content, article, .product, .description, div, section, p")]
            txts = [t for t in txts if len(t) > 120]
            txts.sort(key=len, reverse=True)
            if txts: out["description"] = txts[0]

        full = soup.get_text(" ", strip=True)
        m = re.search(SKU_PAT, full, re.I)
        if m: out["sku"] = out["sku"] or m.group(2)
        m = re.search(GTIN_PAT, full, re.I)
        if m: out["gtin"] = out["gtin"] or m.group(2)
        m = re.search(DDL_PAT, full, re.I)
        if m: out["deadline"] = m.group(2)
        m = re.search(ETA_PAT, full, re.I)
        if m: out["eta"] = m.group(2)

        # HEO affinamenti
        host = urlparse(url).netloc
        if "heo.com" in host:
            m = re.search(r"ETA[:\s]+([A-Za-z]{3,}\s*\d{4}|\d{2}/\d{4}|FINE\s*\d{2}/\d{4})", full, re.I)
            if m: out["eta"] = m.group(1).strip()

    except Exception as e:
        print(f"[WARN] Scrape fallito: {e}")
    # pulizia base
    out["description"] = (out["description"] or "").strip()
    return out

# ---------------- TRANSLATE ----------------
def ensure_english(text):
    if not text: return text
    sample = text[:400]
    try:
        lang = detect(sample)
    except Exception:
        lang = "en"
    if lang == "en":
        return text
    try:
        # MyMemory ha limiti: traduciamo a blocchi da ~2000 char
        chunks = []
        src = "it" if lang == "it" else "auto"
        trans = MyMemoryTranslator(source=src, target="en")
        s = text
        while s:
            chunk = s[:2000]
            s = s[2000:]
            chunks.append(trans.translate(chunk))
        return "\n".join(chunks)
    except Exception as e:
        print(f"[WARN] Traduzione saltata ({e}); mantengo lingua originale.")
        return text

# ---------------- COLLECTIONS ----------------
def list_collections_all():
    url = f"{BASE_V1}/collections/query"
    cursor = None
    out = []
    while True:
        body = {"query": {"paging": {"limit": 100}}}
        if cursor:
            body["query"]["cursorPaging"] = {"cursor": cursor}
        data = http("POST", url, body)
        items = data.get("collections", []) or data.get("items", [])
        out.extend(items)
        cursor = data.get("pagingMetadata", {}).get("cursors", {}).get("next")
        if not cursor:
            break
    return out

def find_collection_best(name):
    if not name: return None
    target = norm(name)
    items = list_collections_all()
    # match esatto
    for c in items:
        if norm(c.get("name","")) == target:
            return c.get("id")
    # match contenuto
    for c in items:
        n = norm(c.get("name",""))
        if target in n or n in target:
            return c.get("id")
    return None

def add_product_to_collection(product_id, collection_id):
    if not collection_id: return
    url = f"{BASE_V1}/collections/{collection_id}/products/add"
    http("POST", url, {"productIds": [product_id]})

# ---------------- DEDUP ----------------
def query_products_page(limit=50, cursor=None):
    url = f"{BASE_V1}/products/query"
    body = {"query":{"paging":{"limit": limit}}}
    if cursor:
        body["query"]["cursorPaging"] = {"cursor": cursor}
    return http("POST", url, body)

def find_existing_product_id(name, slug):
    # cerca per slug e per nome (casefold)
    target_name = norm(name)
    target_slug = norm(slug)
    cursor = None
    for _ in range(20):  # fino a ~1000 items
        data = query_products_page(limit=50, cursor=cursor)
        arr = data.get("products", []) or data.get("items", [])
        for p in arr:
            ps = norm(p.get("slug",""))
            pn = norm(p.get("name",""))
            if ps == target_slug or pn == target_name:
                return p.get("id")
        cursor = data.get("pagingMetadata", {}).get("cursors", {}).get("next")
        if not cursor:
            break
    return None

# ---------------- BUILD PAYLOAD ----------------
def build_payload(row, scraped):
    full_name = row.get("nome_articolo","").strip()
    name80 = limit_len(full_name, 80)

    price = money(row.get("prezzo_eur"))
    sku = (row.get("sku") or scraped.get("sku") or "").strip()
    brand = (row.get("brand") or "").strip() or None

    # descrizione: CSV > scraping
    descr_raw = (row.get("descrizione") or "").strip() or scraped.get("description") or ""
    # traduci in inglese se non è già EN
    descr_en = ensure_english(descr_raw)

    # ETA/Deadline: CSV o scraping
    eta = (row.get("eta") or row.get("eta_trimestre") or row.get("eta_raw") or scraped.get("eta") or "").strip()
    ddl = (row.get("preorder_scadenza") or row.get("deadline_preordine") or row.get("deadline_raw") or scraped.get("deadline") or "").strip()
    banner = build_banner(eta, ddl)

    desc_html = to_html(banner + descr_en)

    base_slug = slugify(name80)
    if sku:
        base_slug = f"{base_slug}-{slugify(sku)}"

    product = {
        "name": name80,
        "slug": base_slug,
        "visible": True,
        "productType": "physical",
        "description": desc_html,
        "priceData": {"currency": CURRENCY, "price": price},
        "manageVariants": True,
        "productOptions": [
            {
                "name": "PREORDER PAYMENTS OPTIONS*",
                "choices": [
                    {"value": "ANTICIPO/SALDO", "description": "Pagamento con acconto 30% e saldo alla consegna"},
                    {"value": "PAGAMENTO ANTICIPATO", "description": "Pagamento anticipato con sconto 5%"}
                ],
            }
        ],
        "ribbon": "PREORDER",
        "seoData": {
            "title": limit_len(full_name, 300),
            "description": limit_len(descr_en, 300),
        },
    }
    if brand:
        product["brand"] = brand
    return product, base_slug, sku, price

# ---------------- CREATE/UPDATE ----------------
def create_product_v1(product):
    url = f"{BASE_V1}/products"
    # 1) wrapper
    r1 = http("POST", url, {"product": product})
    pid = r1.get("id") or r1.get("productId") or (r1.get("product") or {}).get("id")
    if pid: return pid
    # 2) senza wrapper
    r2 = http("POST", url, product)
    pid = r2.get("id") or r2.get("productId") or (r2.get("product") or {}).get("id")
    if pid: return pid
    print("[DEBUG] Create product senza id", json.dumps({"try1":r1, "try2":r2}, ensure_ascii=False)[:800])
    return None

def patch_product_v1(product_id, product):
    url = f"{BASE_V1}/products/{product_id}"
    body = {"product": product}
    http("PATCH", url, body)

def patch_variants_v1(product_id, sku_base, price_eur):
    as_price = round(price_eur * 0.30, 2)
    pa_price = round(price_eur * 0.95, 2)
    body = {
        "variants": [
            {
                "choices": {"PREORDER PAYMENTS OPTIONS*": "ANTICIPO/SALDO"},
                "sku": f"{sku_base}-AS" if sku_base else "",
                "visible": True,
                "priceData": {"currency": CURRENCY, "price": as_price},
            },
            {
                "choices": {"PREORDER PAYMENTS OPTIONS*": "PAGAMENTO ANTICIPATO"},
                "sku": f"{sku_base}-PA" if sku_base else "",
                "visible": True,
                "priceData": {"currency": CURRENCY, "price": pa_price},
            },
        ]
    }
    url = f"{BASE_V1}/products/{product_id}/variants"
    http("PATCH", url, body)

# ---------------- MAIN ----------------
def run(csv_path):
    rows = parse_csv(csv_path)

    # precheck soft
    try:
        probe = http("POST", f"{BASE_V1}/products/query", {"query": {"paging": {"limit": 5}}})
        vis = len([p for p in probe.get("products", []) if p.get("visible")])
        print(f"[PRECHECK] API ok. Prodotti visibili: {vis}")
    except Exception as e:
        print(f"[WARN] Precheck fallito: {e}")

    created = 0
    updated = 0

    for idx, row in enumerate(rows, start=2):
        name = row.get("nome_articolo","").strip()
        print(f"[WORK] Riga {idx}: {name}")

        # scraping sorgente (per descrizione/eta/deadline se mancano)
        scraped = scrape_all(row.get("url_produttore") or row.get("link_url_distributore") or row.get("url") or "")

        product, slug, sku, price = build_payload(row, scraped)

        # dedup: trova prodotto esistente
        existing_id = find_existing_product_id(product["name"], slug)

        try:
            if existing_id:
                # Update
                patch_product_v1(existing_id, product)
                patch_variants_v1(existing_id, sku, price)
                # categoria
                cat_name = (row.get("categoria") or row.get("categoria_wix") or "").strip()
                if cat_name:
                    coll_id = find_collection_best(cat_name)
                    if coll_id:
                        add_product_to_collection(existing_id, coll_id)
                        print(f"[INFO] Aggiornato e assegnato a collection '{cat_name}'")
                    else:
                        print(f"[WARN] Collection '{cat_name}' non trovata.")
                print(f"[OK] Riga {idx} aggiornata '{name}'")
                updated += 1
            else:
                # Create
                pid = create_product_v1(product)
                if not pid:
                    raise RuntimeError("ID prodotto non ricevuto.")
                # varianti
                patch_variants_v1(pid, sku, price)
                # categoria
                cat_name = (row.get("categoria") or row.get("categoria_wix") or "").strip()
                if cat_name:
                    coll_id = find_collection_best(cat_name)
                    if coll_id:
                        add_product_to_collection(pid, coll_id)
                        print(f"[INFO] Assegnato a collection '{cat_name}'")
                    else:
                        print(f"[WARN] Collection '{cat_name}' non trovata.")
                print(f"[OK] Riga {idx} creato '{name}'")
                created += 1

        except Exception as e:
            print(f"[ERRORE] Riga {idx} '{name}': {e}")

        time.sleep(0.2)

    if created == 0 and updated == 0:
        print("[ERRORE] Nessun prodotto creato/aggiornato.")
        sys.exit(2)
    print(f"[FINE] Creati: {created}  Aggiornati: {updated}")

if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--csv", required=True, help="Percorso file CSV (es. input/template_preordini_v7.csv)")
    args = ap.parse_args()
    run(args.csv)
