#!/usr/bin/env python3
import os, sys, csv, re, json, argparse, requests
from bs4 import BeautifulSoup

ARTDIR = "artifacts"
os.makedirs(ARTDIR, exist_ok=True)

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

def prune(x):
    if isinstance(x, dict):
        out = {}
        for k,v in x.items():
            pv = prune(v)
            if pv in (None, "", {}, []): 
                continue
            out[k] = pv
        return out
    if isinstance(x, list):
        out = [prune(v) for v in x]
        return [v for v in out if v not in (None,"",{},[])]
    return x

def read_rows(csv_path):
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
        soup = BeautifulSoup(r.text, "lxml")
        for sel in [".product-description", "#description", ".description", "article", ".product-single__description"]:
            node = soup.select_one(sel)
            if node and node.get_text(strip=True) and len(node.get_text(strip=True))>60:
                out["description"] = str(node); break
        if not out["description"]:
            md = soup.select_one('meta[name="description"]')
            if md and md.get("content"): out["description"] = md.get("content")
        for tag in soup.select('meta[property="og:image"], meta[name="og:image"]'):
            c = tag.get("content")
            if c and c.startswith("http") and c not in out["images"]: out["images"].append(c)
        for img in soup.find_all("img"):
            src = img.get("src") or img.get("data-src") or ""
            if src.startswith("//"): src = "https:"+src
            if src.startswith("http") and src not in out["images"]: out["images"].append(src)
        text = soup.get_text("\n", strip=True)
        m = re.search(r"\b(\d{8,14})\b", text);  out["ean"] = out["ean"] or (m.group(1) if m else None)
        for pat in [r"SKU[:\s]+([A-Z0-9\-\._/]+)", r"Cod(?:ice)?\s*[:\s]+([A-Z0-9\-\._/]+)"]:
            m = re.search(pat, text, re.I)
            if m and not out["sku"]: out["sku"] = m.group(1)
        m = re.search(r"(?:peso|weight)\s*[:\s]+([\d\.,]+)\s*(kg|g)", text, re.I)
        if m:
            val = float(m.group(1).replace(",", "."))
            out["weight_kg"] = val/1000.0 if m.group(2).lower()=="g" else val
    except Exception as e:
        print(f"[WARN] fetch_from_page: {e}")
    return out

def wix_request(method, url, api_key, site_id, payload=None):
    headers = {"Content-Type":"application/json", "Authorization": api_key, "wix-site-id": site_id}
    data = json.dumps(prune(payload)) if payload is not None else None
    r = requests.request(method, url, headers=headers, data=data, timeout=40)
    if r.status_code >= 300:
        raise RuntimeError(f"{method} {url} failed {r.status_code}: {r.text[:1200]}")
    return r.json()

def precheck(api_key, site_id):
    try:
        res = wix_request("POST", "https://www.wixapis.com/stores/v1/products/query", api_key, site_id, {"query":{}})
        items = res.get("products") or res.get("items") or []
        print(f"[PRECHECK] API ok. Prodotti visibili: {len(items)}")
        return True
    except Exception as e:
        print(f"[PRECHECK] FALLITO: {e}")
        return False

def create_product_v1(api_key, site_id, product):
    return wix_request("POST", "https://www.wixapis.com/stores/v1/products", api_key, site_id, {"product": product})

def add_media_v1(api_key, site_id, product_id, urls):
    if not urls: return
    media = [{"src": u} for u in urls[:20] if u.startswith("http")]
    if not media: return
    wix_request("POST", f"https://www.wixapis.com/stores/v1/products/{product_id}/media",
                api_key, site_id, {"mediaItems": media})

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
        res = wix_request("POST","https://www.wixapis.com/stores/v1/collections",
                          api_key, site_id, {"collection":{"name": name}})
        return res.get("collection",{}).get("id") or res.get("id")
    except Exception as e:
        print(f"[WARN] Create collection '{name}': {e}")
        return None

def add_product_to_collection(api_key, site_id, col_id, product_id):
    if not col_id or not product_id: return
    try:
        wix_request("POST", f"https://www.wixapis.com/stores/v1/collections/{col_id}/productIds",
                    api_key, site_id, {"productIds":[product_id]})
    except Exception as e:
        print(f"[WARN] Add to collection: {e}")

def main():
    p = argparse.ArgumentParser()
    p.add_argument("--csv", required=True)
    args = p.parse_args()

    api_key = os.getenv("WIX_API_KEY","").strip()
    site_id = os.getenv("WIX_SITE_ID","").strip()
    dry = os.getenv("DRY_RUN","0").strip() == "1"
    skip_pre = os.getenv("SKIP_PRECHECK","0").strip() == "1"

    if not api_key or not site_id:
        print("Errore: imposta WIX_API_KEY e WIX_SITE_ID come env.")
        sys.exit(1)

    if not skip_pre:
        if not precheck(api_key, site_id):
            print("[INFO] Precheck fallito. Interrompo per evitare sorprese.")
            sys.exit(3)
    else:
        print("[INFO] SKIP_PRECHECK=1: salto il test di lettura e provo direttamente a creare.")

    created = []
    with open(args.csv, "rb") as _f: pass

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
        peso = to_float(r.get("peso_kg"))
        descr= r.get("descrizione") or ""
        tipo = (r.get("tipo_articolo") or "PREORDER").upper()
        is_preorder = (tipo == "PREORDER")

        scraped = fetch_from_page(urlp) if urlp else {}
        if not descr: descr = scraped.get("description") or name
        if peso is None: peso = scraped.get("weight_kg")
        if not sku: sku = scraped.get("sku")
        images = scraped.get("images") or []

        slug = f"{slugify(name)}-{sku.lower()}" if sku else slugify(name)

        # >>>>>>>>>>>>> CAMBIO CHIAVE QUI: productType esplicito <<<<<<<<<<<<<<
        product = {
            "name": name,
            "slug": slug,
            "visible": True,
            "description": descr,
            "productType": "physical",           # <-- obbligatorio in V1
            "priceData": {"price": price}
        }

        if is_preorder:
            dep, full = compute_prices(price)
            product["manageVariants"] = True
            product["productOptions"] = [{
                "name":"PREORDER PAYMENTS OPTIONS",
                "choices":[
                    {"value":"ANTICIPO/SALDO", "description":"Pagamento con acconto 30% e saldo alla consegna"},
                    {"value":"PAGAMENTO ANTICIPATO", "description":"Pagamento anticipato con sconto 5%"}
                ]
            }]
            product["variants"] = [
                {"choices":{"PREORDER PAYMENTS OPTIONS":"ANTICIPO/SALDO"},
                 "priceData":{"price": dep},
                 **({"sku": f"{sku}-DEP"} if sku else {}),
                 **({"weight": peso} if peso is not None else {})},
                {"choices":{"PREORDER PAYMENTS OPTIONS":"PAGAMENTO ANTICIPATO"},
                 "priceData":{"price": full},
                 **({"sku": f"{sku}-FULL"} if sku else {}),
                 **({"weight": peso} if peso is not None else {})}
            ]

        with open(os.path.join(ARTDIR, f"payload_row_{rownum}.json"), "w", encoding="utf-8") as f:
            json.dump({"product": product}, f, ensure_ascii=False, indent=2)

        if dry:
            print(f"[DRY-RUN] Riga {rownum}: simulazione, nessuna creazione.")
            continue

        try:
            res = create_product_v1(api_key, site_id, product)
            pid = res.get("product",{}).get("id")
            with open(os.path.join(ARTDIR, f"response_row_{rownum}.json"), "w", encoding="utf-8") as f:
                json.dump(res, f, ensure_ascii=False, indent=2)
            if not pid:
                print(f"[ERRORE] Riga {rownum}: risposta senza product.id (vedi artifacts/response_row_{rownum}.json)")
                continue
            print(f"[OK] Riga {rownum} creato prodotto id={pid} :: {name}")
        except Exception as e:
            with open(os.path.join(ARTDIR, f"response_row_{rownum}.json"), "w", encoding="utf-8") as f:
                f.write(str(e))
            print(f"[ERRORE] Riga {rownum} '{name}': {e}")
            continue

        try:
            add_media_v1(api_key, site_id, pid, images)
        except Exception as e:
            print(f"[WARN] Immagini non aggiunte: {e}")

        try:
            cat = (r.get("categoria") or "").strip()
            br  = (r.get("brand") or "").strip()
            if cat:
                cid = find_or_create_collection(api_key, site_id, cat)
                add_product_to_collection(api_key, site_id, cid, pid)
            if br:
                bid = find_or_create_collection(api_key, site_id, f"Brand: {br}")
                add_product_to_collection(api_key, site_id, bid, pid)
        except Exception as e:
            print(f"[WARN] Collezioni: {e}")

        created.append({"row": rownum, "id": pid, "name": name, "slug": slug})

    with open("created_products.json", "w", encoding="utf-8") as f:
        json.dump({"created": created}, f, ensure_ascii=False, indent=2)

    if not created and not dry:
        print("[ERRORE] Nessun prodotto creato.")
        sys.exit(2)

if __name__ == "__main__":
    main()
