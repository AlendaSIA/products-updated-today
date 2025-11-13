import os
import time
import json
import datetime as dt

from flask import Flask, jsonify, request
import requests
import xmltodict
from dateutil import tz

# Google Sheets
import gspread
from google.oauth2.service_account import Credentials


BASE_URL = "https://go.paytraq.com/api"

# === Konfigurācija no ENV ===
PAYTRAQ_KEY = os.environ.get("PAYTRAQ_API_KEY", "")
PAYTRAQ_TOKEN = os.environ.get("PAYTRAQ_API_TOKEN", "")

# Service Account JSON (pilns JSON kā string)
GOOGLE_SA_JSON = os.environ.get("GOOGLE_SA_JSON", "")

# Kolonnu saraksts, kādā rakstām Products_FULL
PRODUCT_COLUMNS = [
    "ItemID",
    "Code",
    "Name",
    "Status",
    "Type",
    "BarCode",
    "UnitID",
    "UnitName",
    "GroupID",
    "GroupName",
    "CountryOrigin",
    "CommodityCode",
    "HasImage",
    "HasLots",
    "Weight",
    "OrderLeadTime",
    "Cost",
    "StandardCost",
    "Qty",
    "InterimAvailable",
    "PriceGrossAmount",
    "PriceTaxRate",
    "PriceCurrency",
    "PriceDiscount",
    "SupplierName",
    "SupplierProductCode",
    "SupplierProductName",
    "PurchasePrice",
    "PurchasePriceCurrency",
    "PurchasePriceIncludeTax",
    "SupplierIsDefault",
    "CreatedUTC",
    "UpdatedUTC",
]

app = Flask(__name__)


# ---------- Palīgfunkcijas ----------

def riga_today_start_end_utc():
    """Atgriež (start_utc_iso, end_utc_iso) šodienai Europe/Riga zonā, UTC ISO formātā."""
    riga = tz.gettz("Europe/Riga")
    now_riga = dt.datetime.now(riga)
    start_riga = dt.datetime(now_riga.year, now_riga.month, now_riga.day, 0, 0, 0, tzinfo=riga)
    end_riga = start_riga + dt.timedelta(days=1)
    start_utc = start_riga.astimezone(tz.UTC)
    end_utc = end_riga.astimezone(tz.UTC)
    to_iso = lambda x: x.strftime("%Y-%m-%dT%H:%M:%SZ")
    return to_iso(start_utc), to_iso(end_utc)


def paytraq_get(path, params=None, timeout=30):
    """Vienots GET helperis (headers + query)."""
    if params is None:
        params = {}
    merged_params = dict(params)
    merged_params.setdefault("APIKey", PAYTRAQ_KEY)
    merged_params.setdefault("APIToken", PAYTRAQ_TOKEN)

    headers = {"APIKey": PAYTRAQ_KEY, "APIToken": PAYTRAQ_TOKEN}
    url = f"{BASE_URL.rstrip('/')}/{path.lstrip('/')}"

    try:
        resp = requests.get(url, params=merged_params, headers=headers, timeout=timeout)
        return resp.status_code, resp.text
    except Exception as e:
        return 599, f"REQUEST_ERROR: {repr(e)}"


def parse_products_xml(xml_text):
    """
    Izvelk produktu sarakstu no PayTraq XML (<Products><Product>...</Product></Products>).
    Ja atnāk tukša lapa (<Products/>), atgriež [].
    """
    try:
        data = xmltodict.parse(xml_text)
    except Exception as e:
        raise ValueError(f"XML_PARSE_ERROR: {repr(e)}")

    if "Products" in (data or {}):
        products_node = data.get("Products")
        if products_node in (None, ""):
            return []  # tukša lapa
    else:
        products_node = None

    if products_node is None:
        err_node = (data or {}).get("Error")
        if isinstance(err_node, dict):
            err_msg = " | ".join(f"{k}={v}" for k, v in err_node.items())
        else:
            err_msg = (xml_text or "")[:300].replace("\n", " ")
        raise ValueError(f"NO_PRODUCTS_NODE: {err_msg}")

    items = products_node.get("Product", [])
    if isinstance(items, dict):
        items = [items]
    return items


def fetch_all_products(logger, extra_params=None, max_pages=500, page_sleep=0.5):
    """
    Lapo no page=1 līdz brīdim, kad atnāk tukša lapa (vai sasniegts max_pages).
    extra_params – piem., {"suppliers": "true"}.
    """
    collected, debug, page = [], [], 1
    extra_params = extra_params or {}

    while page <= max_pages:
        params = {"page": page}
        params.update(extra_params)

        status, text = paytraq_get("/products", params=params)
        debug.append(f"page={page} status={status}")

        if status == 401:
            return None, debug + ["Unauthorized (401) — pārbaudi PAYTRAQ_API_KEY / PAYTRAQ_API_TOKEN"]
        if status >= 400:
            snippet = (text or "")[:300].replace("\n", " ")
            return None, debug + [f"HTTP {status} body_snippet={snippet}"]

        try:
            items = parse_products_xml(text)
        except ValueError as e:
            snippet = str(e)[:300]
            return None, debug + [f"PARSE_FAIL {snippet}"]

        if not items:
            break  # tukša lapa – beidzam
        collected.extend(items)
        page += 1
        time.sleep(page_sleep)

    return collected, debug


def get_gspread_client():
    """Atgriež autorizētu gspread klientu no GOOGLE_SA_JSON (ENV)."""
    if not GOOGLE_SA_JSON:
        raise RuntimeError("Missing GOOGLE_SA_JSON env var with Service Account JSON")

    # Pilns service account JSON kā dict
    info = json.loads(GOOGLE_SA_JSON)

    # Scopes: rakstīšana uz Sheets + pieeja Drive
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]
    creds = Credentials.from_service_account_info(info, scopes=scopes)
    return gspread.authorize(creds)


def first_empty_row(ws):
    """Drošāk atrod pirmo tukšo rindu 1. kolonnā (A)."""
    vals = ws.col_values(1)
    return len(vals) + 1 if vals else 1


def ensure_header(ws):
    """
    Pārliecinās, ka 1. rindā ir galvene.
    Ja nav – ieliek PRODUCT_COLUMNS.
    Atgriež headers (list).
    """
    headers = ws.row_values(1)
    if not headers:
        headers = list(PRODUCT_COLUMNS)
        ws.update("A1:" + gspread.utils.rowcol_to_a1(1, len(headers)), [headers])
        return headers

    # Ja kāda no mūsu kolonnām nav – pieliek beigās, bet neko neizmet
    changed = False
    for col in PRODUCT_COLUMNS:
        if col not in headers:
            headers.append(col)
            changed = True
    if changed:
        ws.update("A1:" + gspread.utils.rowcol_to_a1(1, len(headers)), [headers])
    return headers


def read_existing_codes(ws, code_idx):
    """Atgriež set ar visiem jau esošajiem kodiem (no 'Code' kolonnas)."""
    col_vals = ws.col_values(code_idx)
    if not col_vals:
        return set()
    # Pirmā rinda ir galvene
    return set(v.strip() for v in col_vals[1:] if v and v.strip())


def read_code_to_row_map(ws, code_idx):
    """
    Atgriež dict: Code -> row_index (1-based), balstoties uz konkrēto 'Code' kolonu.
    """
    col_vals = ws.col_values(code_idx)
    mapping = {}
    # 1. rinda ir header, sākam no 2
    for i, val in enumerate(col_vals[1:], start=2):
        code = (val or "").strip()
        if code and code not in mapping:
            mapping[code] = i
    return mapping


def normalize_product_full_for_sheet(p):
    """
    Izveido dict ar laukiem, ko liekam Products_FULL sheetā.
    """
    p = p or {}
    unit = p.get("Unit") or {}
    group = p.get("Group") or {}
    tax = p.get("TaxKeys") or {}      # pagaidām nelietojam, bet atstājam kādreizējiem uzlabojumiem
    inv = p.get("Inventory") or {}
    price = p.get("Price") or {}
    ts = p.get("TimeStamps") or {}
    suppliers = (p.get("Suppliers") or {}).get("Supplier", [])
    if isinstance(suppliers, dict):
        suppliers = [suppliers]

    # paņemam default supplier, vai pirmo
    supplier = None
    for s in suppliers:
        if (s.get("IsDefault") or "").strip().lower() == "true":
            supplier = s
            break
    if not supplier and suppliers:
        supplier = suppliers[0]
    supplier = supplier or {}

    return {
        "ItemID": (p.get("ItemID") or "").strip(),
        "Code": (p.get("Code") or "").strip(),
        "Name": (p.get("Name") or "").strip(),
        "Status": (p.get("Status") or "").strip(),
        "Type": (p.get("Type") or "").strip(),
        "BarCode": (p.get("BarCode") or "").strip(),
        "UnitID": (unit.get("UnitID") or "").strip(),
        "UnitName": (unit.get("UnitName") or "").strip(),
        "GroupID": (group.get("GroupID") or "").strip(),
        "GroupName": (group.get("GroupName") or "").strip(),
        "CountryOrigin": (p.get("CountryOrigin") or "").strip(),
        "CommodityCode": (p.get("CommodityCode") or "").strip(),
        "HasImage": (p.get("HasImage") or "").strip(),
        "HasLots": (p.get("HasLots") or "").strip(),
        "Weight": (p.get("Weight") or "").strip(),
        "OrderLeadTime": (p.get("OrderLeadTime") or "").strip(),
        "Cost": (p.get("Cost") or "").strip(),
        "StandardCost": (p.get("StandardCost") or "").strip(),
        "Qty": (inv.get("Qty") or "").strip(),
        "InterimAvailable": (inv.get("InterimAvailable") or "").strip(),
        "PriceGrossAmount": (price.get("GrossAmount") or "").strip(),
        "PriceTaxRate": (price.get("TaxRate") or "").strip(),
        "PriceCurrency": (price.get("Currency") or "").strip(),
        "PriceDiscount": (price.get("Discount") or "").strip(),
        "SupplierName": (supplier.get("SupplierName") or "").strip(),
        "SupplierProductCode": (supplier.get("SupplierProductCode") or "").strip(),
        "SupplierProductName": (supplier.get("SupplierProductName") or "").strip(),
        "PurchasePrice": (supplier.get("PurchasePrice") or "").strip(),
        "PurchasePriceCurrency": (supplier.get("PurchasePriceCurrency") or "").strip(),
        "PurchasePriceIncludeTax": (supplier.get("PurchasePriceIncludeTax") or "").strip(),
        "SupplierIsDefault": (supplier.get("IsDefault") or "").strip(),
        "CreatedUTC": (ts.get("Created") or "").strip(),
        "UpdatedUTC": (ts.get("Updated") or "").strip(),
    }


# ---------- Flask maršruti ----------

@app.get("/")
def health():
    return jsonify({"ok": True, "service": "paytraq-products-test"}), 200


@app.get("/paytraq-raw")
def paytraq_raw():
    """Ātra diagnostika: atgriež PayTraq /products 1. lapas statusu + atbildes snippet."""
    if not PAYTRAQ_KEY or not PAYTRAQ_TOKEN:
        return jsonify({"ok": False, "error": "Missing PAYTRAQ_API_KEY or PAYTRAQ_API_TOKEN"}), 400
    status, text = paytraq_get("/products", params={"page": 1})
    snippet = (text or "")[:300].replace("\n", " ")
    return jsonify({"ok": True, "status": status, "snippet": snippet}), 200


# --- 1) Pilna eksporta maršruts ---

@app.get("/export-all-products-to-sheet")
def export_all_products_to_sheet():
    """
    Izvelk VISUS produktus no PayTraq un pārraksta norādīto worksheet.
    Parametri:
      - spreadsheet_id (obligāts)
      - worksheet (default "Products_FULL")
      - suppliers=1 lai iekļautu piegādātāju info
      - create=1 lai automātiski izveidotu worksheet, ja tāda nav
    """
    if not PAYTRAQ_KEY or not PAYTRAQ_TOKEN:
        return jsonify({"ok": False, "error": "Missing PAYTRAQ_API_KEY or PAYTRAQ_API_TOKEN"}), 400

    spreadsheet_id = request.args.get("spreadsheet_id", "").strip()
    worksheet_name = request.args.get("worksheet", "Products_FULL").strip()
    suppliers_flag = request.args.get("suppliers", "0").strip()
    create_flag = request.args.get("create", "0").strip()

    if not spreadsheet_id:
        return jsonify({"ok": False, "error": "Missing spreadsheet_id query param"}), 400

    extra_params = {"suppliers": "true"} if suppliers_flag == "1" else {}

    items_all, debug = fetch_all_products(app.logger, extra_params=extra_params, max_pages=500, page_sleep=0.5)
    if items_all is None:
        return jsonify({"ok": False, "error": "Fetch failed", "debug": debug}), 502

    products = [normalize_product_full_for_sheet(p) for p in items_all]

    # Sheets
    try:
        gc = get_gspread_client()
        sh = gc.open_by_key(spreadsheet_id)
        try:
            ws = sh.worksheet(worksheet_name)
        except gspread.WorksheetNotFound:
            if create_flag == "1":
                ws = sh.add_worksheet(title=worksheet_name, rows=2000, cols=len(PRODUCT_COLUMNS) + 5)
            else:
                return jsonify({
                    "ok": False,
                    "error": f"Worksheet '{worksheet_name}' not found. Use ?create=1 to auto-create."
                }), 400

        # pilns pārraksts
        headers = list(PRODUCT_COLUMNS)
        data_rows = [[prod.get(col, "") for col in headers] for prod in products]

        ws.clear()
        all_rows = [headers] + data_rows
        end_a1 = gspread.utils.rowcol_to_a1(len(all_rows), len(headers))
        ws.update(f"A1:{end_a1}", all_rows, value_input_option="USER_ENTERED")

    except Exception:
        app.logger.exception("Sheets access error (export-all)")
        return jsonify({"ok": False, "error": "Sheets access error"}), 500

    return jsonify({
        "ok": True,
        "sheet": {"spreadsheet_id": spreadsheet_id, "worksheet": worksheet_name},
        "counts": {"exported": len(products)},
        "actions": ["overwrite write"],
        "debug": debug[-15:],
    }), 200


# --- 2) Šodien izveidoto produktu pievienošana (jauni kodi) ---

@app.get("/sync-today-products-to-sheet")
def sync_today_products_to_sheet():
    """
    Atrod šodien PayTraq izveidotos produktus (pēc CreatedUTC)
    un pievieno tos Products_FULL sheetā, ja koda vēl nav.
    """
    if not PAYTRAQ_KEY or not PAYTRAQ_TOKEN:
        return jsonify({"ok": False, "error": "Missing PAYTRAQ_API_KEY or PAYTRAQ_API_TOKEN"}), 400

    spreadsheet_id = request.args.get("spreadsheet_id", "").strip()
    worksheet_name = request.args.get("worksheet", "Products_FULL").strip()
    suppliers_flag = request.args.get("suppliers", "1").strip()

    if not spreadsheet_id:
        return jsonify({"ok": False, "error": "Missing spreadsheet_id query param"}), 400

    start_iso, end_iso = riga_today_start_end_utc()
    app.logger.info(f"[NEW] Europe/Riga today window => start_utc={start_iso}, end_utc={end_iso}")

    extra_params = {"suppliers": "true"} if suppliers_flag == "1" else {}
    items_all, debug = fetch_all_products(app.logger, extra_params=extra_params, max_pages=500, page_sleep=0.5)
    if items_all is None:
        return jsonify({"ok": False, "error": "Fetch failed", "debug": debug}), 502

    today_products = []
    for p in items_all:
        ts = (p.get("TimeStamps") or {})
        created = (ts.get("Created") or "").strip()
        if created and (start_iso <= created <= end_iso):
            today_products.append(normalize_product_full_for_sheet(p))

    actions = []

    try:
        gc = get_gspread_client()
        sh = gc.open_by_key(spreadsheet_id)
        ws = sh.worksheet(worksheet_name)

        headers = ensure_header(ws)
        if "Code" not in headers:
            return jsonify({"ok": False, "error": "Sheet has no 'Code' column in header"}), 500

        header_index = {h: i for i, h in enumerate(headers)}
        code_idx = header_index["Code"] + 1

        existing_codes = read_existing_codes(ws, code_idx)
        to_append = []

        for prod in today_products:
            code = prod.get("Code", "")
            if not code:
                continue
            if code in existing_codes:
                continue  # jau ir

            # Izveido rindu pēc esošās galvenes
            row = ["" for _ in headers]
            for h, idx in header_index.items():
                if h in prod:
                    row[idx] = prod[h]
            to_append.append(row)

        appended = 0
        if to_append:
            start_row = first_empty_row(ws)
            end_row = start_row + len(to_append) - 1
            end_a1 = gspread.utils.rowcol_to_a1(end_row, len(headers))
            ws.update(f"A{start_row}:{end_a1}", to_append, value_input_option="USER_ENTERED")
            appended = len(to_append)
            actions.append(f"meklēju — atradu {len(today_products)} šodienas produktus, no kuriem {appended} bija jauni un ierakstīti Sheetā.")
        else:
            if today_products:
                actions.append("meklēju — šodien bija jauni produkti, bet visi kodi jau bija Sheetā.")
            else:
                actions.append("meklēju — šodien PayTraq nav izveidotu produktu.")

    except Exception:
        app.logger.exception("Sheets access error (sync-today)")
        return jsonify({"ok": False, "error": "Sheets access error"}), 500

    return jsonify({
        "ok": True,
        "sheet": {"spreadsheet_id": spreadsheet_id, "worksheet": worksheet_name},
        "counts": {"today": len(today_products), "appended": appended},
        "actions": actions,
        "debug": debug[-15:],
    }), 200


# --- 3) Šodien atjaunināto produktu UPDATE Products_FULL sheetā ---

@app.get("/sync-updated-products-to-sheet")
def sync_updated_products_to_sheet():
    """
    Atrod šodien PayTraq atjauninātos produktus (pēc UpdatedUTC)
    un atjaunina atbilstošās rindas Products_FULL sheetā.
    Ja kods nav sheetā, to izlaiž (par jaunajiem rūpējas sync-today-products-to-sheet).
    """
    if not PAYTRAQ_KEY or not PAYTRAQ_TOKEN:
        return jsonify({"ok": False, "error": "Missing PAYTRAQ_API_KEY or PAYTRAQ_API_TOKEN"}), 400

    spreadsheet_id = request.args.get("spreadsheet_id", "").strip()
    worksheet_name = request.args.get("worksheet", "Products_FULL").strip()
    suppliers_flag = request.args.get("suppliers", "1").strip()

    if not spreadsheet_id:
        return jsonify({"ok": False, "error": "Missing spreadsheet_id query param"}), 400

    start_iso, end_iso = riga_today_start_end_utc()
    app.logger.info(f"[UPDATED] Europe/Riga today window => start_utc={start_iso}, end_utc={end_iso}")

    extra_params = {"suppliers": "true"} if suppliers_flag == "1" else {}
    items_all, debug = fetch_all_products(app.logger, extra_params=extra_params, max_pages=500, page_sleep=0.5)
    if items_all is None:
        return jsonify({"ok": False, "error": "Fetch failed", "debug": debug}), 502

    updated_products_raw = []
    for p in items_all:
        ts = (p.get("TimeStamps") or {})
        updated = (ts.get("Updated") or "").strip()
        if updated and (start_iso <= updated <= end_iso):
            updated_products_raw.append(p)

    try:
        gc = get_gspread_client()
        sh = gc.open_by_key(spreadsheet_id)
        ws = sh.worksheet(worksheet_name)

        headers = ensure_header(ws)
        if "Code" not in headers:
            return jsonify({"ok": False, "error": "Sheet has no 'Code' column in header"}), 500

        header_index = {h: i for i, h in enumerate(headers)}
        code_idx = header_index["Code"] + 1
        code_to_row = read_code_to_row_map(ws, code_idx)

    except Exception:
        app.logger.exception("Sheets access error (updated)")
        return jsonify({"ok": False, "error": "Sheets access error"}), 500

    updated_rows = 0
    skipped_not_found = 0
    updates_debug = []

    for p in updated_products_raw:
        prod = normalize_product_full_for_sheet(p)
        code = prod.get("Code", "")
        if not code:
            continue

        row_idx = code_to_row.get(code)
        if not row_idx:
            skipped_not_found += 1
            updates_debug.append(f"skip code={code} (not in sheet)")
            continue  # šos lai savāc 'new products' job

        # pašreizējās rindas vērtības
        row_vals = ws.row_values(row_idx)
        if len(row_vals) < len(headers):
            row_vals += [""] * (len(headers) - len(row_vals))

        # pārrakstām tikai tos headerus, kurus zinām
        for h, idx in header_index.items():
            if h in prod:
                row_vals[idx] = prod[h]

        end_a1 = gspread.utils.rowcol_to_a1(row_idx, len(headers))
        ws.update(f"A{row_idx}:{end_a1}", [row_vals], value_input_option="USER_ENTERED")
        updated_rows += 1
        updates_debug.append(f"updated code={code} row={row_idx}")

    actions = []
    if not updated_products_raw:
        actions.append("meklēju — šodien PayTraq nav atjauninātu produktu.")
    else:
        actions.append(
            f"meklēju — PayTraq atradu {len(updated_products_raw)} atjauninātus produktus, "
            f"no tiem {updated_rows} rindas aktualizēju Sheetā, {skipped_not_found} kodi nebija atrodami."
        )

    return jsonify({
        "ok": True,
        "sheet": {"spreadsheet_id": spreadsheet_id, "worksheet": worksheet_name},
        "counts": {
            "updated_today": len(updated_products_raw),
            "rows_updated": updated_rows,
            "skipped_not_found": skipped_not_found,
        },
        "actions": actions,
        "debug": (debug or [])[-10:] + updates_debug[:20],
    }), 200


# ---------- Flask app start ----------

if __name__ == "__main__":
    port = int(os.environ.get("PORT", "8080"))
    app.run(host="0.0.0.0", port=port)
