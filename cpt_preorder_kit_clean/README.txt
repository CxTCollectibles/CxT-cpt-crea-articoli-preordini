# CPT Preorder Kit — Automazione Wix

## Cosa contiene
- `wix_preorder_ingestion.py` — Importer: legge il CSV, fa scraping dal link produttore, crea il prodotto su Wix, assegna alle collezioni (Categoria e Brand).
- `.github/workflows/importa_preordini.yml` — Workflow GitHub Actions con 3 modalità di input (percorso nel repo, URL esterno, oppure contenuto incollato).
- `input/` — cartella per caricare i CSV via commit/push.

## Requisiti
- Due Secrets nel repo (Settings → Secrets and variables → Actions):
  - `WIX_API_KEY` (permesso *Gestisci prodotti / Manage Products*)
  - `WIX_SITE_ID` (ID del tuo sito)

## Come usarlo
1. Copia questi file nel tuo repo (mantieni il percorso `.github/workflows/importa_preordini.yml`).
2. Vai su **Actions → Importa Preordini su Wix → Run workflow** e usa una di queste opzioni:
   - **csv_path**: percorso nel repo (es. `input/mio.csv`).
   - **csv_url**: URL pubblico (il workflow lo scarica).
   - **csv_inline**: incolla il contenuto CSV direttamente nel campo.
   Se non compili nulla, prenderà **l’ultimo CSV** presente in `input/`.
3. Leggi i log: vedrai gli ID dei prodotti creati ed eventuali avvisi.

## CSV
Colonne richieste: `nome_articolo, prezzo_eur, url_produttore, sku, gtin_ean, peso_kg, descrizione, tipo_articolo, preorder_option, brand, categoria`.

Note utili:
- Separatore `;` o `,` accettato automaticamente.
- Colonne ausiliarie del foglio Excel che iniziano con `__` vengono ignorate.
- Per **PREORDER** lo script crea **sempre** due varianti cliente:
  - `ANTICIPO/SALDO` = 30% del prezzo base
  - `PAGAMENTO ANTICIPATO` = prezzo base − 5%
- Immagini/sku/ean/peso/descrizione: se non indicati nel CSV, vengono presi dalla pagina del produttore.

## Collezioni
- Il prodotto viene aggiunto alla collezione **Categoria** e alla collezione **Brand: {brand}**.
- Se la collezione non esiste, viene creata e poi popolata.

Buon lavoro.
