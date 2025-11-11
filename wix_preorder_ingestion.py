#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os, sys, csv, re, json, argparse, unicodedata
from typing import Dict, Any, List, Optional
import requests
from bs4 import BeautifulSoup

# ---------- Config ----------
API_KEY = os.environ.get("WIX_API_KEY", "").strip()
SITE_ID = os.environ.get("WIX_SITE_ID", "").strip()
BASE = "https://www.wixapis.com"

HEADERS_RW = {
    "Authorization": API_KEY,
    "wix-site-id": SITE_ID,
    "Content-Type": "application/json"
}
HEADERS_R = {
    "Authorization": API_KEY,
    "wix-site-id": SITE_ID
}

OPTION_TITLE = "PREORDER PAYMENTS OPTIONS*"
CHOICE_AS = "ANTICIPO/SALDO"
CHOICE_PA = "PAGAMENTO ANTICIPATO"
NAME_MAX = 80  # limite Wix

# ---------- Utils ----------
def log(s): print(s, flush=True)

def slugify(text):
    text = unicodedata.normalize("NFKD", text)
    text = "".join([c for c in text if not unicodedata.combining(c)])
    text = re.sub(r"[^a-zA-Z0-9]+", "-", text).strip("-").lower()
    return text[:100] or "item"

def money2(x: float) -> float:
    return round(float(x)+1e-8, 2)

def parse_price(txt: str) -> float:
    if not txt: return 0.0
    t = txt.strip().replace("€","").replace(",",".")
    m = re.search(r"(\d+(?:\.\d+)?)", t)
    return float(m.group(1)) if m else 0.0

def normalize_whitespace(s: str) -> str:
    return re.sub(r"\s+", " ", s or "").strip()

# ---------- HTTP ----------
def req_json(method: str, url: str, headers: Dict[str,str], body: Optional[Dict]=None):
    try:
        if method == "GET":
            r = requests.get(url, headers=headers, timeout=30)
        elif method == "POST":
            r = requests.post(url, headers=headers, data=json.dumps(body or {}), timeout=60)
        elif method == "PATCH":
            r = requests.patch(url, headers=headers, data=json.dumps(body or {}), timeout=60)
        else:
            raise RuntimeError("HTTP method non supportato")
        code = r.status_code
        try:
            data = r.json()
        except Exception:
            data = {}
        return code, data
    except Exception as e:
        return 0, {"error": str(e)}

# ---------- CSV ----------
def sniff_csv(csv_path: str):  # Excel -> ; + utf-8-sig
    return "utf-8-sig", ";"

def read_rows(csv_path: str) -> List[Dict[str,str]]:
    enc, delim = sniff_csv(csv_path)
    log(f"[INFO] CSV encoding={enc} delimiter='{delim}'")
    rows = []
    with open(csv_path, "r", encoding=enc, newline="") as f:
        rdr = csv.DictReader(f, delimiter=delim)
        for r in rdr:
            rr = { (k or "").strip().lower(): (v or "").strip() for k,v in r.items() }
            rows.append(rr)
    return rows

# ---------- Collections ----------
def list_collections() -> List[Dict[str,Any]]:
    url = f"{BASE}/stores-reader/v1/collections/query"
    body = {"query": {"paging": {"limit": 100}}}
    code, data = req_json("POST", url, HEADERS_R, body)
    if code != 200:
        log(f"[WARN] Collections query: {code} {json.dumps(data)}")
        return []
    return data.get("collections", [])

def find_collection_id_by_name(name: str) -> Optional[str]:
    wanted = normalize_whitespace(name).lower()
    for c in list_collections():
        nm = normalize_whitespace(c.get("name","")).lower()
        if nm == wanted:
            return c.get("id")
    return None

def add_product_to_collection(col_id: str, product_id: str) -> bool:
    # API corretta: aggiunge IDs alla collection
    url = f"{BASE}/stores/v1/collections/{col_id}/productIds"
    body = {"ids": [product_id]}
    code, data = req_json("POST", url, HEADERS_RW, body)
    return code == 200

# ---------- Descrizione ----------
def fetch_description_from_url(url: str) -> str:
    try:
        r = requests.get(url, timeout=30, headers={"User-Agent":"Mozilla/5.0"})
        if r.status_code != 200:
            return ""
        soup = BeautifulSoup(r.text, "lxml")

        meta = soup.find("meta", attrs={"name":"description"})
        if meta and meta.get("content"):
            md = normalize_whitespace(meta["content"])
            if 40 <= len(md) <= 5000:
                return md

        candidates = []
        for sel in [
            "div.product-description","div#product-description","section.description","div#description",
            "div[itemprop=description]","div.product-details","div.product__description"
        ]:
            el = soup.select_one(sel)
            if el:
                txt = normalize_whitespace(el.get_text(" ", strip=True))
                if len(txt) > 60:
                    candidates.append(txt)
        if candidates:
            candidates.sort(key=len, reverse=True)
            return candidates[0][:5000]

        ps = soup.find_all(["p","div"])
        best = ""
        for el in ps:
            txt = normalize_whitespace(el.get_text(" ", strip=True))
            if 80 <= len(txt) <= 5000 and len(txt) > len(best):
                best = txt
        return best
    except Exception:
        return ""

def build_description(hdr: str, raw: str) -> str:
    parts = []
    if hdr: parts.append(hdr)
    if raw: parts.append(raw)
    body = "\n\n".join(parts).strip()
    if not body: return ""
    body = body.replace("&","&amp;").replace("<","&lt;").replace(">","&gt;").replace("\n","<br>")
    return "<div><p>" + body + "</p></div>"

# ---------- Create/Patch ----------
def create_product(product_payload: Dict[str,Any]) -> Optional[str]:
    # Wix vuole il wrapper {"product": {...}}
    url = f"{BASE}/stores/v1/products"
    code, data = req_json("POST", url, HEADERS_RW, {"product": product_payload})
    if code == 200 and data.get("product",{}).get("id"):
        return data["product"]["id"]
    return None

def patch_product(product_id: str, patch: Dict[str,Any]) -> bool:
    url = f"{BASE}/stores/v1/products/{product_id}"
    code, _ = req_json("PATCH", url, HEADERS_RW, {"product": patch})
    return code == 200

def set_variants(product_id: str, variants: List[Dict[str,Any]]) -> bool:
    url = f"{BASE}/stores/v1/products/{product_id}/variants"
    code, _ = req_json("PATCH", url, HEADERS_RW, {"variants": variants})
    return code == 200

# ---------- Main ----------
def run(csv_path: str):
    if not API_KEY or not SITE_ID:
        log("[ERRORE] WIX_API_KEY o WIX_SITE_ID mancanti nei secrets.")
        sys.exit(2)

    # precheck lettura (permessi)
    code, data = req_json("POST", f"{BASE}/stores-reader/v1/products/query", HEADERS_R, {"query":{"paging":{"limit":5}}})
    if code != 200:
        log(f"[ERRORE] PRECHECK fallito: {code} {json.dumps(data)}")
        sys.exit(2)
    log(f"[PRECHECK] API ok. Prodotti visibili: {len(data.get('products',[]))}")

    rows = read_rows(csv_path)
    if not rows:
        log("[ERRORE] CSV vuoto o non valido.")
        sys.exit(2)

    # info collections (aiuta a verificare i nomi)
    cols = list_collections()
    if cols:
        names = ", ".join([c.get("name","") for c in cols][:20])
        log(f"[INFO] Collections disponibili (prime 20): {names}")

    created = 0
    for i, r in enumerate(rows, start=2):
        name = r.get("nome articolo") or r.get("nome_articolo") or r.get("name") or ""
        prezzo_txt = r.get("prezzo") or r.get("prezzo_eur") or ""
        if not name or not prezzo_txt:
            log(f"[SKIP] Riga {i}: nome_articolo o prezzo_eur mancante.")
            continue

        price = parse_price(prezzo_txt)
        is_preorder = (r.get("preordine") or r.get("preorder") or "si").strip().lower() in ("si","sì","yes","true","1","preorder")

        brand = r.get("brand") or r.get("marca") or ""
        sku = r.get("sku") or r.get("codice prodotto") or r.get("codice") or ""
        gtin = r.get("gtin") or r.get("ean") or ""
        peso = parse_price(r.get("peso") or r.get("peso_grammi") or "0")
        categoria_csv = r.get("categoria") or r.get("collection") or r.get("categoria wix") or ""

        url_sorg = r.get("link al sito del produttore") or r.get("url_prodotto") or r.get("url") or ""
        descr_csv = r.get("descrizione") or ""
        deadline_csv = r.get("deadline") or r.get("scadenza") or r.get("data scadenza") or r.get("chiusura preordine") or ""
        eta_csv = r.get("eta") or r.get("uscita prevista") or r.get("release") or r.get("data uscita") or ""

        log(f"[WORK] Riga {i}: {name}")

        # Nome <= 80
        if len(name) > NAME_MAX:
            log(f"[WARN] Nome > {NAME_MAX} caratteri, troncato.")
        name80 = name[:NAME_MAX]

        # Header (ETA/Deadline) in descrizione
        hdr_parts = []
        if deadline_csv: hdr_parts.append(f"Preorder closes: {deadline_csv}")
        if eta_csv:      hdr_parts.append(f"ETA: {eta_csv}")
        hdr = " — ".join(hdr_parts)

        raw_descr = (descr_csv or "").strip()
        if not raw_descr and url_sorg:
            raw_descr = fetch_description_from_url(url_sorg).strip()
        desc_html = build_description(hdr, raw_descr)

        # Varianti prezzo
        anticipo = money2(price * 0.30)
        anticipato = money2(price * 0.95)

        slug = slugify(name80 if not sku else f"{name80}-{sku}")
        product = {
            "name": name80,
            "slug": slug,
            "visible": True,
            "productType": "physical",
            "description": desc_html,
            "priceData": {"price": money2(price)},
            "ribbon": "PREORDER" if is_preorder else ""
        }
        if sku:  product["sku"] = sku
        if gtin: product["gtin"] = gtin
        if peso > 0: product["weight"] = peso

        # Opzione + varianti (già in create)
        product["productOptions"] = [{
            "name": OPTION_TITLE,
            "choices": [
                {"value": CHOICE_AS, "description": "Paga 30% ora, saldo alla disponibilità"},
                {"value": CHOICE_PA, "description": "Pagamento immediato con sconto 5%"}
            ]
        }]
        product["manageVariants"] = True
        product["variants"] = [
            {"choices": {OPTION_TITLE: CHOICE_AS}, "priceData": {"price": anticipo}, "visible": True},
            {"choices": {OPTION_TITLE: CHOICE_PA}, "priceData": {"price": anticipato}, "visible": True}
        ]

        pid = create_product(product)

        # fallback: crea senza varianti, poi patcha opzioni e varianti
        if not pid:
            minimal = {k:v for k,v in product.items() if k not in ("productOptions","manageVariants","variants")}
            code, data = req_json("POST", f"{BASE}/stores/v1/products", HEADERS_RW, {"product": minimal})
            if code == 200 and data.get("product",{}).get("id"):
                pid = data["product"]["id"]
                # patch opzioni
                patch_ok = patch_product(pid, {
                    "productOptions": product["productOptions"],
                    "manageVariants": True
                })
                # set varianti
                var_ok = set_variants(pid, product["variants"])
                if not (patch_ok and var_ok):
                    log(f"[WARN] Varianti/opzioni non applicate completamente per '{name80}'.")
            else:
                log(f"[ERRORE] Riga {i} '{name80}': POST /products failed {code}: {json.dumps(data)}")
                continue

        # Brand: se vuoi, mappa su ribbon/description. Campo Brand nativo via REST non sempre è esposto.
        # Assegna categoria se presente
        if categoria_csv:
            col_id = find_collection_id_by_name(categoria_csv)
            if col_id:
                if add_product_to_collection(col_id, pid):
                    log(f"[INFO] Assegnato a collection '{categoria_csv}'")
                else:
                    log(f"[WARN] Add to collection '{categoria_csv}' fallito (permessi/ID?).")
            else:
                log(f"[WARN] Collection '{categoria_csv}' non trovata, il prodotto non sarà categorizzato.")

        log(f"[OK] Riga {i} creato/aggiornato '{name80}' | Varianti: AS={anticipo}  PA={anticipato}")
        created += 1

    if created == 0:
        log("[ERRORE] Nessun prodotto creato/aggiornato.")
        sys.exit(2)
    log(f"[FINE] Prodotti creati/aggiornati: {created}")
    sys.exit(0)

if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--csv", required=True, help="Percorso del CSV (sotto input/)")
    args = ap.parse_args()
    run(args.csv)
