#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os, sys, csv, re, json, time, html
from datetime import datetime
from urllib.parse import urlparse
import requests
from bs4 import BeautifulSoup

# ====== SECRETS ======
WIX_API_KEY = os.environ.get("WIX_API_KEY")
WIX_SITE_ID = os.environ.get("WIX_SITE_ID")

API_BASE = "https://www.wixapis.com"
HDRS = {
    "Authorization": WIX_API_KEY or "",
    "wix-site-id": WIX_SITE_ID or "",
    "Content-Type": "application/json"
}

# ====== UTIL ======

def die(msg, code=2):
    print(f"[ERRORE] {msg}")
    sys.exit(code)

def slugify(s):
    s = s.lower()
    s = re.sub(r"[^\w\s-]+", "", s)
    s = re.sub(r"[\s_-]+", "-", s).strip("-")
    return s[:70]

def money(x):
    try:
        v = float(str(x).replace(",", "."))
    except:
        v = 0.0
    return float(f"{max(0, v):.2f}")

def clean_text(t):
    if not t: return ""
    t = html.unescape(t)
    t = t.replace("\r", "\n")
    t = re.sub(r"\n{3,}", "\n\n", t)
    t = re.sub(r"[ \t]{2,}", " ", t)
    return t.strip()

def quarter_from_date_or_text(eta_text):
    """ 'FINE 08/2026' o 'Aug 2026' o '2026-08' -> ('Q3 - Q4', datetime) """
    if not eta_text:
        return None, None
    t = eta_text.strip()
    # mm/yyyy
    m = re.search(r"(\d{1,2})/(\d{4})", t)
    month = None; year = None
    if m:
        month = int(m.group(1)); year = int(m.group(2))
    else:
        MMM = {"jan":1,"feb":2,"mar":3,"apr":4,"may":5,"jun":6,
               "jul":7,"aug":8,"sep":9,"oct":10,"nov":11,"dec":12}
        m2 = re.search(r"([A-Za-z]{3,})\s+(\d{4})", t)
        if m2:
            month = MMM.get(m2.group(1).lower()[:3]); year = int(m2.group(2))
    if month and year:
        q = (month-1)//3 + 1
        qr = f"Q{q}"
        if "FINE" in t.upper() or month in (3,6,9,12):
            qr = f"Q{q} - Q{min(4,q+1)}" if q<4 else "Q4"
        return qr, datetime(year, month, 1)
    return None, None

def prepend_eta_deadline(desc, eta_text, deadline_text):
    head = []
    if eta_text:
        q,_ = quarter_from_date_or_text(eta_text)
        if q: head.append(f"Uscita prevista: {q}")
    if deadline_text:
        head.append(f"Deadline preordine: {deadline_text}")
    if head:
        banner = "<p><strong>" + " | ".join(head) + "</strong></p>"
        return banner + (desc or "")
    return desc

# ====== SCRAPING GENERICO + ALCUNI DOMINI ======

SKU_PATTERNS = r"(Codice Prodotto|Codice articolo|Artikelnummer|SKU)\s*[:#]?\s*([A-Z0-9\-_.]{4,})"
GTIN_PATTERNS = r"(GTIN|EAN|UPC)\s*[:#]?\s*(\d{8,14})"
DEADLINE_PATTERNS = r"(Deadline|Scadenza|Order Deadline|Preorder deadline)\s*[:#]?\s*([0-3]?\d\.[01]?\d\.\d{4})"
ETA_PATTERNS = r"(ETA|Uscita prevista|Release|Disponibile da)\s*[:#]?\s*([A-Za-z]{3,}\s*\d{4}|\d{2}/\d{4}|FINE\s*\d{2}/\d{4})"

def extract_ld_json(soup):
    data = {}
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
                data.setdefault("name", obj.get("name"))
                data.setdefault("sku", obj.get("sku"))
                data.setdefault("gtin", obj.get("gtin13") or obj.get("gtin") or obj.get("gtin14") or obj.get("gtin8"))
                data.setdefault("description", obj.get("description"))
        pick(j)
    return data

def longest_text_from(container):
    if not container: return ""
    chunks = []
    for p in container.find_all(["p","li","div","span"], recursive=True):
        txt = p.get_text(" ", strip=True)
        if txt and len(txt) > 40:
            chunks.append(txt)
    return "\n".join(chunks)

def scrape_generic(url):
    r = requests.get(url, timeout=25, headers={"User-Agent":"Mozilla/5.0"})
    r.raise_for_status()
    soup = BeautifulSoup(r.text, "html.parser")

    out = {"description":"", "sku":None, "gtin":None, "eta":None, "deadline":None}
    ld = extract_ld_json(soup)
    out.update({k:v for k,v in ld.items() if v})

    # fallback description
    if not out.get("description"):
        og = soup.find("meta", {"property":"og:description"}) or soup.find("meta", {"name":"description"})
        if og and og.get("content"): out["description"] = og["content"]
    # se ancora poco, prendi il blocco più ricco
    if not out.get("description") or len(out["description"]) < 200:
        main = soup.select_one("main") or soup.select_one("#content") or soup
        cand = longest_text_from(main)
        if len(cand) > len(out.get("description","")):
            out["description"] = cand

    # estrazioni testuali generiche
    full_txt = soup.get_text(" ", strip=True)

    msku = re.search(SKU_PATTERNS, full_txt, re.I)
    if msku: out.setdefault("sku", msku.group(2))

    mgt = re.search(GTIN_PATTERNS, full_txt, re.I)
    if mgt: out.setdefault("gtin", mgt.group(2))

    md = re.search(DEADLINE_PATTERNS, full_txt, re.I)
    if md: out.setdefault("deadline", md.group(2))

    me = re.search(ETA_PATTERNS, full_txt, re.I)
    if me: out.setdefault("eta", me.group(2))

    # HEO: affinamenti
    host = urlparse(url).netloc
    if "heo.com" in host:
        # descrizione centrale (meglio del generico)
        center = soup.select_one("#content") or soup
        cand = longest_text_from(center)
        if len(cand) > len(out.get("description","")):
            out["description"] = cand

        # badge ETA arancione spesso contiene 'ETA: FINE 08/2026'
        m = re.search(r"ETA[:\s]+([A-Za-z]{3,}\s*\d{4}|\d{2}/\d{4}|FINE\s*\d{2}/\d{4})", full_txt, re.I)
        if m: out["eta"] = m.group(1).strip()

    # Sideshow: il JSON-LD di solito basta, altrimenti itemprop=description
    if "sideshow" in host:
        if not out.get("description"):
            d = soup.select_one("[itemprop='description']")
            if d: out["description"] = clean_text(d.get_text("\n", strip=True))[:4000]

    out["description"] = clean_text(out.get("description"))
    return out

# ====== WIX HELPERS ======

def wix_list_collections():
    u = f"{API_BASE}/stores/v1/collections/query"
    body = {"query": {"paging":{"limit":200}}}
    r = requests.post(u, headers=HDRS, data=json.dumps(body), timeout=30)
    if not r.ok:
        print(f"[WARN] Collections query: {r.status_code} {r.text}")
        return []
    return r.json().get("items", [])

def best_collection_id_by_name(wanted):
    wanted_n = wanted.strip().casefold()
    items = wix_list_collections()
    # match esatto casefold
    for it in items:
        if it.get("name","").casefold() == wanted_n:
            return it["id"]
    # match parziale
    for it in items:
        if wanted_n in it.get("name","").casefold():
            return it["id"]
    # niente
    return None

def create_wix_product(payload):
    u = f"{API_BASE}/stores/v1/products"
    r = requests.post(u, headers=HDRS, data=json.dumps(payload), timeout=60)
    if not r.ok:
        raise RuntimeError(f"POST /products failed {r.status_code}: {r.text}")
    return r.json().get("id")

# ====== PAYLOAD ======

def build_payload(row, scraped):
    name   = (row["nome_articolo"] or "").strip()
    price  = money(row["prezzo_eur"])
    brand  = (row.get("brand") or "").strip() or None
    sku    = (row.get("sku") or scraped.get("sku") or slugify(name).upper())[:80]
    gtin   = (row.get("gtin_ean") or scraped.get("gtin") or "").strip() or None

    weight = None
    if row.get("peso_kg"):
        try: weight = float(str(row["peso_kg"]).replace(",", "."))
        except: pass

    descr = clean_text(row.get("descrizione") or scraped.get("description") or "")
    # limitiamo per sicurezza
    if len(descr) > 8000: descr = descr[:8000]

    eta  = row.get("eta_raw") or scraped.get("eta")
    ddl  = row.get("deadline_raw") or scraped.get("deadline")
    descr_final = prepend_eta_deadline(descr, eta, ddl)

    # varianti con prezzo
    prezzo_anticipo = money(price * 0.30)
    prezzo_prepag   = money(price * 0.95)

    # collection
    col_id = None
    cat = (row.get("categoria") or "").strip()
    if cat:
        col_id = best_collection_id_by_name(cat)
        if not col_id:
            print(f"[WARN] Collection '{cat}' non trovata, il prodotto non sarà categorizzato.")

    payload = {
        "name": name,
        "slug": f"{slugify(name)}-{sku.lower()}",
        "productType": "physical",
        "description": f"<div><p>{descr_final.replace('\n','<br>')}</p></div>" if descr_final else "",
        "priceData": { "currency": "EUR", "price": price },
        "manageVariants": True,
        "productOptions": [{
            "name": "PREORDER PAYMENTS OPTIONS*",
            "choices": [
                {"value": "ANTICIPO/SALDO", "description": "Pagamento con acconto 30% e saldo alla consegna"},
                {"value": "PAGAMENTO ANTICIPATO", "description": "Pagamento anticipato con sconto 5%"}
            ]
        }],
        "variants": [
            {
                "choices": {"PREORDER PAYMENTS OPTIONS*": "ANTICIPO/SALDO"},
                "sku": f"{sku}-AS",
                "priceData": {"currency":"EUR","price": prezzo_anticipo},
                "visible": True
            },
            {
                "choices": {"PREORDER PAYMENTS OPTIONS*": "PAGAMENTO ANTICIPATO"},
                "sku": f"{sku}-PA",
                "priceData": {"currency":"EUR","price": prezzo_prepag},
                "visible": True
            }
        ],
        "ribbons": ["PREORDER"],
        "visible": True
    }

    if col_id:
        payload["collectionIds"] = [col_id]

    if weight:
        payload["physicalProperties"] = { "weight": { "value": weight, "unit": "kg" } }

    if brand:
        payload["brand"] = brand  # campo avanzato brand, se esposto nel tuo tema

    if gtin:
        payload.setdefault("seoData", {})
        payload["seoData"]["structuredData"] = [{
            "type": "Product",
            "description": name,
            "sku": sku,
            "gtin13": gtin
        }]

    return payload

# ====== CSV E MAIN ======

def parse_csv(path):
    rows = []
    with open(path, newline="", encoding="utf-8-sig") as f:
        rdr = csv.DictReader(f)
        for i, r in enumerate(rdr, start=2):
            if not (r.get("nome_articolo") and r.get("prezzo_eur")):
                print(f"[SKIP] Riga {i}: nome_articolo o prezzo_eur mancante.")
                continue
            rows.append(r)
    return rows

def run(csv_path):
    if not WIX_API_KEY or not WIX_SITE_ID:
        die("Manca WIX_API_KEY o WIX_SITE_ID nei secrets.")

    rows = parse_csv(csv_path)
    if not rows:
        die("CSV vuoto o non valido.", 2)

    created = 0
    for idx, row in enumerate(rows, start=2):
        name = row["nome_articolo"].strip()
        print(f"[WORK] Riga {idx}: {name}")

        scraped = {}
        url = (row.get("url_produttore") or "").strip()
        if url:
            try:
                scraped = scrape_generic(url)
            except Exception as e:
                print(f"[WARN] Scrape fallito: {e}")

        payload = build_payload(row, scraped)

        try:
            pid = create_wix_product(payload)
            created += 1
            v = payload["variants"]
            print(f"[OK] Creato '{name}' | Varianti -> AS={v[0]['priceData']['price']}  PA={v[1]['priceData']['price']}")
        except Exception as e:
            print(f"[ERRORE] Riga {idx} '{name}': {e}")

        time.sleep(0.3)

    if created == 0:
        die("Nessun prodotto creato.", 2)
    print(f"[FINE] Prodotti creati: {created}")

if __name__ == "__main__":
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument("--csv", required=True)
    args = ap.parse_args()
    run(args.csv)
