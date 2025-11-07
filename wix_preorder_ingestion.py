#!/usr/bin/env python3
import os, sys, csv, re, json, argparse, requests
from bs4 import BeautifulSoup

# ---------- Utilità ----------
def slugify(s):
    import unicodedata, re
    s = unicodedata.normalize("NFKD", str(s)).encode("ascii","ignore").decode("ascii")
    s = re.sub(r"[^a-zA-Z0-9]+","-", s).strip("-").lower()
    return s[:80] or "articolo"

def to_float(s):
    if s is None: return None
    s = str(s).strip().replace(",", ".")
    try:
        return float(s)
    except:
        m = re.search(r"([\d\.]+)\s*(kg|g)", s, re.I)
        if m:
            v = float(m.group(1))
            return v/1000.0 if m.group(2).lower()=="g" else v
    return None

def compute_prices(price):
    price = float(price)
    return round(price*0.30,2), round(price*0.95,2)

def read_rows(csv_path):
    # Accetta ; , o tab e ignora colonne ausiliarie tipo __BRANDS__
    with open(csv_path, newline="", encoding="utf-8-sig") as f:
        sample = f.read(4096); f.seek(0)
        try:
            dialect = csv.Sniffer().sniff(sample, delimiters=[",",";","\t"])
        except Exception:
            class _D: delimiter = ","
            dialect = _D()
        reader = csv.DictReader(f, dialect=dialect)
        count = 0
        for i,row in enumerate(reader, start=2):
            row = {k:v for k,v in row.items() if not str(k).startswith("__")}
            norm = {k.strip().lower(): (v or "").strip() for k,v in row.items()}
            if not (norm.get("nome_articolo") or norm.get("prezzo_eur") or norm.get("url_produttore")):
                continue
            count += 1
            yield i, norm
        if count == 0:
            print("[WARN] Nessuna riga valida trovata nel CSV.")

def fetch_from_page(url):
    out = {"images":[], "sku":None, "ean":None, "weight_kg":None, "description":None}
    try:
        headers = {"User-Agent":"Mozilla/5.0 (compatible; CPT-Importer/1.0)"}
        r = requests.get(url, headers=headers, timeout=20)
        r.raise_for_status()
        html = r.text
        soup = BeautifulSoup(html, "lxml")

        # Description
        for sel in [".product-description", "#description", ".description", "article", ".product-single__description"]:
            node = soup.select_one(sel)
            if node and node.get_text(strip=True) and len(node.get_text(strip=True))>60:
                out["description"] = str(node)
                break
        if not out["description"]:
            md = soup.select_one('meta[name="description"]')
            if md and md.get("content"):
                out["description"] = md.get("content")

        # Images
        for tag in soup.select('meta[property="og:image"], meta[name="og:image"]'):
            c = tag.get("content")
            if c and c.startswith("http") and c not in out["images"]:
                out["images"].append(c)
        for img in soup.find_all("img"):
            src = img.get("src") or img.get("data-src") or ""
            if src.startswith("//"): src = "https:"+src
            if src.startswith("http") and src not in out["images"]:
                out["images"].append(src)

        # Text search for EAN/SKU/weight
        text = soup.get_text("\n", strip=True)
        m = re.search(r"\b(\d{13})\b", text)
        if m: out["ean"] = m.group(1)
        for pat in [r"SKU[:\s]+([A-Z0-9\-\._/]+)", r"Cod(?:ice)?\s*[:\s]+([A-Z0-9\-\._/]+)"]:
            m = re.search(pat, text, re.I)
            if m and not out["sku"]:
                out["sku"] = m.group(1)
        m = re.search(r"(?:peso|weight)\s*[:\s]+([\d\.,]+)\s*(kg|g)", text, re.I)
        if m:
            val = float(m.group(1).replace(",", "."))
            out["weight_kg"] = val/1000.0 if m.group(2).lower()=="g" else val
    except Exception as e:
        print(f"[WARN] fetch_from_page: {e}")
    return out

def wix_request(method, url, api_key, site_id, payload=None):
    headers = {"Content-Type":"application/json", "Authorization": api_key, "wix-site-id": site_id}
    r = requests.request(method, url, headers=headers, data=(json.dumps(payload) if payload else None), timeout=30)
    if r.status_code >= 300:
        raise RuntimeError(f"{method} {url} failed {r.status_code}: {r.text[:300]}")
    return r.json()

def find_or_create_collection(api_key, site_id, name):
    try:
        res = wix_request("POST","https://www.wixapis.com/stores/v1/collections/query", api_key, site_id,
                          {"query":{"filter": json.dumps({"name":{"$eq": name}})}, "paging":{"limit":100}})
        items = res.get("collections",[]) or res.get("items",[])
        for c in items:
            if c.get("name","").strip().lower() == name.strip().lower():
                return c.get("id")
    except Exception as e:
        print(f"[WARN] Query collections: {e}")
    try:
        res = wix_request("POST","https://www.wixapis.com/stores/v1/collections", api_key, site_id, {"collection":{"name": name}})
        return res.get("collection",{}).get("id") or res.get("id")
    except Exception as e:
        print(f"[WARN] Create collection '{name}': {e}")
        return None

def add_product_to_collection(api_key, site_id, col_id, product_id):
    if not col_id or not product_id: return
    try:
        wix_request("POST", f"https://www.wixapis.com/stores/v1/collections/{col_id}/productIds", api_key, site_id, {"productIds":[product_id]})
    except Exception as e:
        print(f"[WARN] Add to collection: {e}")

# ---------- Main ----------
def main():
    p = argparse.ArgumentParser()
    p.add_argument("--csv", required=True)
    args = p.parse_args()

    api_key = os.getenv("WIX_API_KEY","").strip()
    site_id = os.getenv("WIX_SITE_ID","").strip()
    if not api_key or not site_id:
        print("Errore: imposta WIX_API_KEY e WIX_SITE_ID come secrets/env.")
        sys.exit(1)

    created = []
    for rownum, r in read_rows(args.csv):
        name = r.get("nome_articolo")
        try:
            price = float((r.get("prezzo_eur") or "0").replace(",",".")); assert price>0
        except:
            print(f"[ERRORE] Riga {rownum}: prezzo non valido"); continue

        urlp = r.get("url_produttore")
        if not re.match(r"^https?://", urlp or ""):
            print(f"[ERRORE] Riga {rownum}: url_produttore non valido"); continue

        sku  = r.get("sku") or None
        ean  = r.get("gtin_ean") or None
        peso = to_float(r.get("peso_kg"))
        descr= r.get("descrizione") or ""
        tipo = (r.get("tipo_articolo") or "PREORDER").upper()
        is_preorder = tipo == "PREORDER"

        scraped = fetch_from_page(urlp) if urlp else {}
        if not descr: descr = scraped.get("description") or ""
        if peso is None: peso = scraped.get("weight_kg")
        ean = ean or scraped.get("ean")
        sku = sku or scraped.get("sku")

        dep, full = compute_prices(price) if is_preorder else (None, None)
        slug = f"{slugify(name)}-{sku.lower()}" if sku else slugify(name)

        product = {
            "name": name,
            "slug": slug,
            "visible": True,
            "description": descr,
            "price": price,
            "inventory": {"trackQuantity": False},
            "manageVariants": bool(is_preorder),
            "ribbon": "PREORDER" if is_preorder else "",
            "mediaItems": [{"src": u} for u in (scraped.get("images") or [])[:10]]
        }
        if is_preorder:
            product["productOptions"] = [{
                "name":"PREORDER PAYMENTS OPTIONS",
                "choices":[{"value":"ANTICIPO/SALDO"},{"value":"PAGAMENTO ANTICIPATO"}]
            }]
            product["variants"] = [
                {"choices":{"PREORDER PAYMENTS OPTIONS":"ANTICIPO/SALDO"}, "price": dep,  "sku": f"{sku}-DEP" if sku else None,  "weight": peso},
                {"choices":{"PREORDER PAYMENTS OPTIONS":"PAGAMENTO ANTICIPATO"}, "price": full, "sku": f"{sku}-FULL" if sku else None, "weight": peso}
            ]
        else:
            if peso is not None: product["weight"] = peso
            product["productOptions"] = []
            product["variants"] = []

        custom = []
        if ean: custom.append({"name":"EAN","value":ean})
        if sku: custom.append({"name":"SKU","value":sku})
        if custom: product["customTextFields"] = custom

        try:
            res = wix_request("POST","https://www.wixapis.com/stores/v1/products", api_key, site_id, {"product": product})
            pid = res.get("product",{}).get("id")
            if not pid:
                print(f"[ERRORE] Riga {rownum}: risposta senza product.id -> {json.dumps(res)[:300]}")
                continue
            print(f"[OK] Riga {rownum} creato prodotto id={pid} :: {name}")
            created.append({"row": rownum, "id": pid, "name": name, "slug": slug})
        except Exception as e:
            print(f"[ERRORE] Riga {rownum} '{name}': {e}")
            continue

        cat = (r.get("categoria") or "").strip()
        br  = (r.get("brand") or "").strip()
        if cat:
            cid = find_or_create_collection(api_key, site_id, cat)
            add_product_to_collection(api_key, site_id, cid, pid)
        if br:
            bid = find_or_create_collection(api_key, site_id, f"Brand: {br}")
            add_product_to_collection(api_key, site_id, bid, pid)

    # Salva riepilogo creazioni
    with open("created_products.json", "w", encoding="utf-8") as f:
        json.dump({"created": created}, f, ensure_ascii=False, indent=2)

    if not created:
        print("[ERRORE] Nessun prodotto creato.")
        sys.exit(2)

# ---------- Entry ----------
if __name__ == "__main__":
    main()
