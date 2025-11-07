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
        m = re.search(r"\b(\d{8,14})\b", text)
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
        raise RuntimeError(f"{method} {url} failed {r.status_code}: {r.text[:500]}")
    return r.json()

# ---------- Creator: prova V3 e fallback V1 ----------
def create_product_dual(api_key, site_id, p, is_preorder, peso):
    # Payload stile V3
    p_v3 = {
        "product": {
            "name": p["name"],
            "slug": p["slug"],
            "visible": True,
            "productType": "physical",
            "description": p.get("description") or "",
            "media": {"items": [{"src": u} for u in (p.get("images") or [])[:10]]},
        }
    }
    if is_preorder:
        p_v3["product"]["ribbon"] = "PREORDER"
        p_v3["product"]["productOptions"] = [{
            "name": "PREORDER PAYMENTS OPTIONS",
            "choices": [{"value":"ANTICIPO/SALDO"}, {"value":"PAGAMENTO ANTICIPATO"}]
        }]
        # Varianti con priceData
        dep, full = compute_prices(p["price"])
        p_v3["product"]["variants"] = [
            {"choices":{"PREORDER PAYMENTS OPTIONS":"ANTICIPO/SALDO"}, "priceData":{"price": dep},  "sku": f"{p['sku']}-DEP" if p.get("sku") else None, "physicalProperties":{"weight": peso} if peso is not None else None},
            {"choices":{"PREORDER PAYMENTS OPTIONS":"PAGAMENTO ANTICIPATO"}, "priceData":{"price": full}, "sku": f"{p['sku']}-FULL" if p.get("sku") else None, "physicalProperties":{"weight": peso} if peso is not None else None}
        ]
    else:
        p_v3["product"]["priceData"] = {"price": p["price"]}
        if peso is not None:
            p_v3["product"]["physicalProperties"] = {"weight": peso}

    # Prova V3
    try:
        res = wix_request("POST", "https://www.wixapis.com/stores/v3/products", api_key, site_id, p_v3)
        pid = res.get("product",{}).get("id")
        if pid:
            print("[INFO] Creato via Catalog V3")
            return pid, res
        else:
            print("[WARN] V3 senza product.id, provo V1… ->", str(res)[:200])
    except Exception as e:
        print("[WARN] V3 fallita, provo V1…", e)

    # Payload stile V1 (minimal-compat)
    p_v1 = {
        "product": {
            "name": p["name"],
            "slug": p["slug"],
            "visible": True,
            "description": p.get("description") or "",
            "mediaItems": [{"src": u} for u in (p.get("images") or [])[:10]],
        }
    }
    if is_preorder:
        dep, full = compute_prices(p["price"])
        p_v1["product"]["ribbons"] = [{"text":"PREORDER"}]
        p_v1["product"]["productOptions"] = [{
            "name": "PREORDER PAYMENTS OPTIONS",
            "choices": [{"value":"ANTICIPO/SALDO"}, {"value":"PAGAMENTO ANTICIPATO"}]
        }]
        p_v1["product"]["manageVariants"] = True
        p_v1["product"]["variants"] = [
            {"choices":{"PREORDER PAYMENTS OPTIONS":"ANTICIPO/SALDO"},
