#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import os, sys, csv, re, json, argparse, requests, unicodedata
from bs4 import BeautifulSoup

ARTDIR = "artifacts"
os.makedirs(ARTDIR, exist_ok=True)

# ====== Opzioni / varianti ======
OPTION_NAME = "PREORDER PAYMENTS OPTIONS*"
CHOICE_DEPOSIT = "ANTICIPO/SALDO"
CHOICE_FULLPAY = "PAGAMENTO ANTICIPATO"

# Alias categorie comode
CATEGORY_ALIASES = {
    "statue": "Statue da collezione",
    "statua": "Statue da collezione",
    "statue da collezione": "Statue da collezione",
    "action figures": "Action Figures da Collezione",
    "repliche": "Repliche Cinematografiche",
}

# ---------------- Utils ----------------
def slugify(s):
    s = unicodedata.normalize("NFKD", str(s)).encode("ascii","ignore").decode("ascii")
    s = re.sub(r"[^a-zA-Z0-9]+","-", s).strip("-").lower()
    return s[:80] or "articolo"

def to_float_kg(s):
    if s is None: return None
    s = str(s).strip()
    if not s: return None
    try:
        return float(s.replace(",", "."))
    except:
        m = re.search(r"([\d\.]+)\s*(kg|g)\b", s, re.I)
        if m:
            v = float(m.group(1).replace(",", "."))
            return v/1000.0 if m.group(2).lower()=="g" else v
    return None

def compute_prices(base):
    base = float(base)
    deposito = round(base * 0.30, 2)
    anticipato = round(base * 0.95, 2)
    return deposito, anticipato

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

def normalize_name(s):
    if not s: return ""
    s = unicodedata.normalize("NFKD", s).encode("ascii","ignore").decode("ascii")
    s = re.sub(r"\s+", " ", s).strip().lower()
    return s

# ---------------- CSV ----------------
def read_rows(csv_path):
    # Prova automatica delimiter , ; \t
    with open(csv_path, newline="", encoding="utf-8-sig") as f:
        sample = f.read(4096); f.seek(0)
        try:
            dialect = csv.Sniffer().sniff(sample, delimiters=[",",";","\t"])
        except Exception:
            class _D: delimiter = ";"
            dialect = _D()
        reader = csv.DictReader(f, dialect=dialect)
        count = 0
        for i,row in enumerate(reader, start=2):
            row = {k:v for k,v in row.items() if k is not None and not str(k).startswith("__")}
            norm = { (k or "").strip().lower(): (v or "").strip() for k,v in row.items() }
            if not (norm.get("nome_articolo") or norm.get("prezzo_eur") or norm.get("url_produttore")):
                continue
            count += 1
            yield i, norm
        if count == 0:
            print("[WARN] Nessuna riga valida trovata nel CSV.")

# ---------------- Scraping ----------------
MONTHS = r"(jan(?:uary)?|feb(?:ruary)?|mar(?:ch)?|apr(?:il)?|may|jun(?:e)?|jul(?:y)?|aug(?:ust)?|sep(?:t(?:ember)?)?|oct(?:ober)?|nov(?:ember)?|dec(?:ember)?|gen(?:naio)?|feb(?:braio)?|mar(?:zo)?|apr(?:ile)?|mag(?:gio)?|giu(?:gno)?|lug(?:lio)?|ago(?:sto)?|set(?:tembre)?|ott(?:obre)?|nov(?:embre)?|dic(?:embre)?|jan|feb|märz|apr|mai|jun|jul|aug|sep|okt|nov|dez)"

def extract_eta_deadline(text):
    txt = " ".join(text.split())
    eta = None
    deadline = None
    # Qx - Qy 20xx
    m = re.search(r"\bQ[1-4]\s*-\s*Q[1-4]\s*20\d{2}\b", txt, re.I)
    if m: eta = m.group(0)
    if not eta:
        m = re.search(r"\bQ[1-4]\s*20\d{2}\b", txt, re.I)
        if m: eta = m.group(0)
    # Month Year
    if not eta:
        m = re.search(rf"\b{MONTHS}\s+20\d{{2}}\b", txt, re.I)
        if m: eta = m.group(0).title()
    # Phrasal
    if not eta:
        m = re.search(r"(release|eta|uscita|rilascio|lieferung|voraussicht)\s*[:\-]?\s*([A-Za-z0-9\s\-\./]+20\d{2})", txt, re.I)
        if m:
            cand = m.group(2).strip()
            cand = re.split(r"[;,]|(\s{2,})", cand)[0]
            eta = cand
    # Deadline dd/mm/yyyy or dd.mm.yyyy
    m = re.search(r"(pre[-\s]?order|chiusura\s+preordine|order\s+deadline|bestellschluss)\s*[:\-]?\s*(\d{1,2}[./]\d{1,2}[./]\d{2,4})", txt, re.I)
    if m:
        deadline = m.group(2).replace(".", "/")
        parts = deadline.split("/")
        if len(parts[-1]) == 2: parts[-1] = "20"+parts[-1]
        deadline = "/".join(parts)
    return eta, deadline

def prefer_english(texts):
    # Sceglie il testo che sembra più "inglese" e lungo
    def score(t):
        t0 = t.lower()
        eng_hits = sum(1 for w in ["the","and","with","figure","statue","scale","includes","features","inch","cm","preorder"] if w in t0)
        return (eng_hits*50) + len(t)
    texts = [t for t in texts if t and len(t.strip()) > 60]
    if not texts: return None
    texts.sort(key=score, reverse=True)
    return texts[0]

def extract_description(soup: BeautifulSoup):
    # 1) JSON-LD
    for tag in soup.find_all("script", type="application/ld+json"):
        try:
            data = json.loads(tag.string or "")
            if isinstance(data, list):
                for d in data:
                    if isinstance(d, dict) and str(d.get("@type","")).lower() == "product" and d.get("description"):
                        return d["description"]
            elif isinstance(data, dict) and str(data.get("@type","")).lower() == "product" and data.get("description"):
                return data["description"]
        except Exception:
            pass
    # 2) Meta
    for sel,attr,val in [('meta','property','og:description'), ('meta','name','description'), ('meta','property','og:description:secure')]:
        el = soup.find(sel, attrs={attr: val})
        if el and el.get("content"): 
            return el.get("content")
    # 3) Blocchi "sotto le foto"
    cands = []
    selectors = [
        ".product-description", "#description", ".description", "article",
        ".product-single__description", ".product-info__description", ".short-description",
        "#tab-description", "[data-tab='description']", ".tabs-content", ".tab-content",
        ".product__description", ".desc"
    ]
    for sel in selectors:
        n = soup.select_one(sel)
        if n:
            txt = n.get_text("\n", strip=True)
            if txt and len(txt) > 80:
                cands.append(txt)
    if cands:
        best = prefer_english(cands) or max(cands, key=len)
        return best
    # 4) Paragrafi lunghi
    longp = [p.get_text(" ", strip=True) for p in soup.find_all("p")]
    longp = [t for t in longp if len(t) > 120]
    if longp:
        return prefer_english(longp) or max(longp, key=len)
    return None

def fetch_from_page(url):
    out = {"images":[], "sku":None, "ean":None, "weight_kg":None, "description":None, "eta":None, "deadline":None}
    try:
        headers = {"User-Agent":"Mozilla/5.0 (compatible; CPT-Importer/1.0)"}
        r = requests.get(url, headers=headers, timeout=25)
        r.raise_for_status()
        soup = BeautifulSoup(r.text, "lxml")

        out["description"] = extract_description(soup)

        # Immagini: og:image / secure, <img>, <picture><source srcset>, data-zoom-image, link a .jpg/.png
        seen = set()
        def ok(u):
            u = (u or "").strip()
            u2 = u.lower()
            bad = ("thumb", "thumbnail", "small_", "/small/", "icon", "placeholder", "lazy", "50x50", "100x100", "150x150")
            return u.startswith("http") and not any(b in u2 for b in bad)

        for key in ["og:image:secure_url", "og:image"]:
            q = soup.find("meta", attrs={"property": key}) or soup.find("meta", attrs={"name": key})
            if q and q.get("content"):
                u = q.get("content").strip()
                if ok(u) and u not in seen: out["images"].append(u); seen.add(u)

        for img in soup.find_all("img"):
            src = img.get("src") or img.get("data-src") or img.get("data-original") or img.get("data-image") or img.get("data-large") or img.get("data-zoom-image") or ""
            if src.startswith("//"): src = "https:"+src
            if ok(src) and src not in seen:
                out["images"].append(src); seen.add(src)

        for src in soup.select("source[srcset]"):
            ss = src.get("srcset") or ""
            for part in ss.split(","):
                u = part.strip().split(" ")[0]
                if u.startswith("//"): u = "https:"+u
                if ok(u) and u not in seen:
                    out["images"].append(u); seen.add(u)

        for a in soup.select("a[href]"):
            href = a.get("href","")
            if any(href.lower().endswith(ext) for ext in [".jpg",".jpeg",".png",".webp"]):
                if href.startswith("//"): href = "https:"+href
                if ok(href) and href not in seen:
                    out["images"].append(href); seen.add(href)

        text = soup.get_text("\n", strip=True)
        m = re.search(r"\b(\d{8,14})\b", text);  out["ean"] = m.group(1) if m else None
        for pat in [r"SKU[:\s]+([A-Z0-9\-\._/]+)", r"Cod(?:ice)?\s*[:\s]+([A-Z0-9\-\._/]+)"]:
            m = re.search(pat, text, re.I)
            if m and not out["sku"]:
                out["sku"] = m.group(1)
        m = re.search(r"(?:peso|weight)\s*[:\s]+([\d\.,]+)\s*(kg|g)\b", text, re.I)
        if m:
            val = float(m.group(1).replace(",", "."))
            out["weight_kg"] = val/1000.0 if m.group(2).lower()=="g" else val

        e, d = extract_eta_deadline(text)
        out["eta"], out["deadline"] = e, d

    except Exception as e:
        print(f"[WARN] fetch_from_page: {e}")
    return out

# ---------------- Wix API ----------------
def wix_request(method, url, api_key, site_id, payload=None):
    headers = {"Content-Type":"application/json", "Authorization": api_key, "wix-site-id": site_id}
    data = json.dumps(prune(payload)) if payload is not None else None
    r = requests.request(method, url, headers=headers, data=data, timeout=45)
    if r.status_code >= 300:
        raise RuntimeError(f"{method} {url} failed {r.status_code}: {r.text[:1200]}")
    if r.text.strip():
        try:
            return r.json()
        except Exception:
            return {}
    return {}

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
    urls = [u for u in urls if isinstance(u,str) and u.startswith("http")]
    if not urls: return
    items = [{"src": u} for u in urls[:20]]
    try:
        wix_request("POST", f"https://www.wixapis.com/stores/v1/products/{product_id}/media",
                    api_key, site_id, {"mediaItems": items})
    except Exception as e:
        print(f"[WARN] Add media failed: {e}")

def list_collections(api_key, site_id):
    try:
        res = wix_request("POST","https://www.wixapis.com/stores/v1/collections/query",
                          api_key, site_id, {"query":{"paging":{"limit": 500}}})
        return (res.get("collections") or res.get("items") or [])
    except Exception as e:
        print(f"[WARN] Query collections: {e}")
        return []

def list_categories(api_key, site_id):
    try:
        res = wix_request("POST","https://www.wixapis.com/stores/v1/categories/query",
                          api_key, site_id, {"query":{"paging":{"limit": 500}}})
        return (res.get("categories") or res.get("items") or [])
    except Exception:
        return []

def find_category_or_collection_id(api_key, site_id, name):
    if not name: return None
    target = CATEGORY_ALIASES.get(name.strip().lower(), name).strip()
    norm_target = normalize_name(target)

    items = list_collections(api_key, site_id) + list_categories(api_key, site_id)
    for c in items:
        nm = (c.get("name") or "").strip()
        if normalize_name(nm) == norm_target:
            return c.get("id")
    # ultimo tentativo: startswith
    for c in items:
        nm = (c.get("name") or "").strip()
        if normalize_name(nm).startswith(norm_target):
            return c.get("id")
    return None

def add_product_to_collection(api_key, site_id, col_id, product_id):
    if not col_id or not product_id: return
    try:
        wix_request("POST", f"https://www.wixapis.com/stores/v1/collections/{col_id}/productIds",
                    api_key, site_id, {"productIds":[product_id]})
    except Exception as e:
        print(f"[WARN] Add to collection: {e}")

# ---------------- Main ----------------
def parse_image_list_field(s):
    if not s: return []
    parts = [p.strip() for p in str(s).split("|")]
    return [p for p in parts if p.startswith("http")]

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

    for rownum, r in read_rows(args.csv):
        name = r.get("nome_articolo") or r.get("name")
        urlp = r.get("url_produttore") or r.get("link_al_sito_del_produttore")
        if not re.match(r"^https?://", urlp or ""):
            print(f"[ERRORE] Riga {rownum}: url_produttore non valido"); 
            continue
        try:
            price = float((r.get("prezzo_eur") or r.get("prezzo") or "0").replace(",","."))
            assert price > 0
        except:
            print(f"[ERRORE] Riga {rownum}: prezzo non valido"); 
            continue

        sku = r.get("sku") or None
        peso = to_float_kg(r.get("peso_kg"))
        descr = r.get("descrizione") or ""
        brand = r.get("brand") or ""
        categoria = r.get("categoria") or ""
        categoria = CATEGORY_ALIASES.get(categoria.strip().lower(), categoria).strip()

        preorder_deadline = r.get("preorder_scadenza") or ""
        eta = r.get("eta") or ""
        images_from_csv = parse_image_list_field(r.get("immagini_urls"))
        visible = (r.get("visibile_online") or "SI").strip().upper() != "NO"
        tipo = (r.get("tipo_articolo") or "PREORDER").strip().upper()
        is_preorder = (tipo == "PREORDER")

        scraped = fetch_from_page(urlp)
        if not descr: descr = scraped.get("description") or name
        if not eta and scraped.get("eta"): eta = scraped["eta"]
        if not preorder_deadline and scraped.get("deadline"): preorder_deadline = scraped["deadline"]
        if peso is None: peso = scraped.get("weight_kg")
        if not sku: sku = scraped.get("sku")
        images = images_from_csv or scraped.get("images") or []

        # prepend “Uscita prevista / Chiusura preordine” in cima alla descrizione
        header_lines = []
        if eta:
            header_lines.append(f"Uscita prevista: {eta}")
        if preorder_deadline:
            header_lines.append(f"Chiusura preordine : {preorder_deadline} Salvo esaurimento")
        if header_lines:
            header_block = "\n".join(header_lines)
            descr = f"{header_block}\n\n{descr}"

        slug = f"{slugify(name)}-{sku.lower()}" if sku else slugify(name)

        product = {
            "name": name,
            "slug": slug,
            "visible": visible,
            "productType": "physical",
            "description": descr,
            "priceData": {"price": price},
            "brand": brand or None,
            "mediaItems": [{"src": u} for u in images[:10] if u.startswith("http")] or None,
            "ribbon": "PREORDINE" if is_preorder else None,
            "manageVariants": True if is_preorder else False
        }

        if is_preorder:
            deposito, anticipato = compute_prices(price)
            product["productOptions"] = [{
                "name": OPTION_NAME,
                "choices": [
                    {"value": CHOICE_DEPOSIT, "description": CHOICE_DEPOSIT},
                    {"value": CHOICE_FULLPAY, "description": CHOICE_FULLPAY}
                ]
            }]
            product["variants"] = [
                {"choices": {OPTION_NAME: CHOICE_DEPOSIT},
                 "priceData": {"price": deposito},
                 **({"sku": f"{sku}-DEP"} if sku else {}),
                 **({"weight": peso} if peso is not None else {})},
                {"choices": {OPTION_NAME: CHOICE_FULLPAY},
                 "priceData": {"price": anticipato},
                 **({"sku": f"{sku}-FULL"} if sku else {}),
                 **({"weight": peso} if peso is not None else {})}
            ]

        # Salva payload per debug
        with open(os.path.join(ARTDIR, f"payload_row_{rownum}.json"), "w", encoding="utf-8") as f:
            json.dump({"product": product}, f, ensure_ascii=False, indent=2)

        if dry:
            print(f"[DRY-RUN] Riga {rownum}: simulazione, nessuna creazione.")
            continue

        # Crea il prodotto
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

        # Foto in fallback (se non importate in creazione)
        try:
            if images:
                add_media_v1(api_key, site_id, pid, images)
        except Exception as e:
            print(f"[WARN] Immagini non aggiunte (fallback): {e}")

        # Categoria esistente
        try:
            if categoria:
                cid = find_category_or_collection_id(api_key, site_id, categoria)
                if cid:
                    add_product_to_collection(api_key, site_id, cid, pid)
                else:
                    print(f"[WARN] Collezione/Categoria '{categoria}' non trovata. Non assegno categorie.")
        except Exception as e:
            print(f"[WARN] Collezioni: {e}")

        created.append({"row": rownum, "id": pid, "name": name, "slug": slug})

    with open("created_products.json", "w", encoding="utf-8") as f:
        json.dump({"created": created}, f, ensure_ascii=False, indent=2)

    if not created and os.getenv("DRY_RUN","0").strip() != "1":
        print("[ERRORE] Nessun prodotto creato.")
        sys.exit(2)

if __name__ == "__main__":
    main()
