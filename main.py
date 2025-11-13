import os
import time
import json
import datetime as dt
from typing import List, Dict, Tuple

import requests
import xmltodict
from dateutil import tz
from flask import Flask, jsonify, request

import gspread
from google.oauth2.service_account import Credentials
from gspread.utils import rowcol_to_a1

# =========================
# ENV konfigurācija
# =========================

PAYTRAQ_KEY   = os.environ.get("PAYTRAQ_API_KEY", "")
PAYTRAQ_TOKEN = os.environ.get("PAYTRAQ_API_TOKEN", "")

BASE_URL = "https://go.paytraq.com/api"
GOOGLE_SA_JSON = os.environ.get("GOOGLE_SA_JSON", "")

app = Flask(__name__)


# =========================
# Laika palīgfunkcijas
# =========================

def riga_today_start_end_utc() -> Tuple[str, str]:
    """
    Atgriež (start_utc_iso, end_utc_iso) šodienai Europe/Riga zonā, ISO UTC formātā.
    """
    riga = tz.gettz("Europe/Riga")
    now_riga = dt.datetime.now(riga)
    start_riga = dt.datetime(now_riga.year, now_riga.month, now_riga.day, 0, 0, 0, tzinfo=riga)
    end_riga = start_riga + dt.timedelta(days=1)

    start_utc = start_riga.astimezone(tz.UTC)
    end_utc = end_riga.astimezone(tz.UTC)

    to_iso = lambda x: x.strftime("%Y-%m-%dT%H:%M:%SZ")
    return to_iso(start_utc), to_iso(end_utc)


# =========================
# PayTraq palīgfunkcijas
# =========================

def paytraq_get(path: str, params: Dict = None, timeout: int = 30) -> Tuple[int, str]:
    """
    GET helperis uz PayTraq API.
    """
    if params is None:
        params = {}
    merged_params = dict(params)
    merged_params.setdefault("APIKey", PAYTRAQ_KEY)
    merged_params.setdefault("APIToken", PAYTRAQ_TOKEN)

    headers = {
        "APIKey": PAYTRAQ_KEY,
        "APIToken": PAYTRAQ_TOKEN
    }

    url = f"{BASE_URL.rstrip('/')}/{path.lstrip('/')}"
    try:
        resp = requests.get(url, params=merged_params, headers=headers, timeout=timeout)
        return resp.status_code, resp.text
    except Exception as e:
        return 599, f"REQUEST_ERROR: {repr(e)}"


def parse_products_xml(xml_text: str) -> List[Dict]:
    """
    Atgriež produktu sarakstu no XML (<Products><Product>..).
    Ja atnāk tukša lapa — atgriež [].
    """
    data = xmltodict.parse(xml_text)
    products_node = (data or {}).get("Products")
    if products_node in (None, ""):
        return []

    items = products_node.get("Product", [])
    if isinstance(items, dict):
        items = [items]
    return items


def normalize_product(p: Dict, want_suppliers: bool) -> Dict:
    """
    Pilns produkta dict (balstīts uz tavu Products_FULL formātu).
    """
    stamps = (p.get("TimeStamps") or {})
    inv = (p.get("Inventory") or {})
    price = (p.get("Price") or {})

    # Suppliers
    sup_list = None
    if want_suppliers:
        sup_list = ((p.get("Suppliers") or {}).get("Supplier"))
        if isinstance(sup_list, dict):
            sup_list = [sup_list]
    sup = None
    if sup_list:
        sup = next((s for s in sup_list if (s.get("IsDefault") or "").strip().lower() == "true"), sup_list[0])

    return {
        "ItemID": (p.get("ItemID") or "").strip(),
        "Code": (p.get("Code") or "").strip(),
        "Name": (p.get("Name") or "").strip(),
        "Status": (p.get("Status") or "").strip(),
        "Type": (p.get("Type") or "").strip(),
        "BarCode": (p.get("BarCode") or "").strip(),
        "GroupName": ((p.get("Group") or {}).get("GroupName") or "").strip(),
        "CountryOrigin": (p.get("CountryOrigin") or "").strip(),
        "CommodityCode": (p.get("CommodityCode") or "").strip(),
        "HasLots": (p.get("HasLots") or "").strip(),
        "Qty": (inv.get("Qty") or "").strip(),
        "InterimAvailable": (inv.get("InterimAvailable") or "").strip(),
        "GrossAmount": (price.get("GrossAmount") or "").strip(),
        "TaxRate": (price.get("TaxRate") or "").strip(),
        "Currency": (price.get("Currency") or "").strip(),
        "Discount": (price.get("Discount") or "").strip(),
        "SupplierName": (sup.get("SupplierName") if sup else "") or "",
        "SupplierProductCode": (sup.get("SupplierProductCode") if sup else "") or "",
        "SupplierProductName": (sup.get("SupplierProductName") if sup else "") or "",
        "PurchasePrice": (sup.get("PurchasePrice") if sup else "") or "",
        "PurchasePriceCurrency": (sup.get("PurchasePriceCurrency") if sup else "") or "",
        "PurchasePriceIncludeTax": (sup.get("PurchasePriceIncludeTax") if sup else "") or "",
        "SupplierIsDefault": (sup.get("IsDefault") if sup else "") or "",
        "CreatedUTC": (stamps.get("Created") or "").strip(),
        "UpdatedUTC": (stamps.get("Updated") or "").strip(),
    }


def fetch_all_products(want_suppliers: bool = False,
                       max_pages: int = 500,
                       page_sleep: float = 0.4) -> Tuple[List[Dict], List[str]]:
    """
    Lapo cauri /products un savāc VISUS produktus.
    """
    collected: List[Dict] = []
    debug: List[str] = []
    page = 1

    base_params: Dict[str, str] = {}
    if want_suppliers:
        base_params["suppliers"] = "true"

    while page <= max_pages:
        params = dict(base_params)
        params["page"] = page

        status, text = paytraq_get("/products", params=params)
        debug.append(f"page={page} status={status}")

        if status == 401:
            debug.append("Unauthorized (401) — pārbaudi PAYTRAQ_API_KEY / PAYTRAQ_API_TOKEN")
            return None, debug

        if status >= 400:
            snippet = (text or "")[:300].replace("\n", " ")
            debug.append(f"HTTP {status} body_snippet={snippet}")
            return None, debug

        items = parse_products_xml(text)
        if not items:
            break

        collected.extend(items)
        page += 1
        time.sleep(page_sleep)

    return collected, debug


# =========================
# Google Sheets palīgfunkcijas
# =========================

def get_gspread_client():
    if not GOOGLE_SA_JSON:
        raise RuntimeError("Missing GOOGLE_SA_JSON env var with Service Account JSON")
    info = json.loads(GOOGLE_SA_JSON)
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]
    creds = Credentials.from_service_account_info(info, scopes=scopes)
    return gspread.authorize(creds)


def ensure_headers(ws) -> List[str]:
    """
    Paņem 1. rindas galvenes. Ja tukšas – izveido ar default (Products_FULL) sarakstu.
    """
    headers = ws.row_values(1)
    if headers:
        return headers

    headers = [
        "ItemID", "Code", "Name", "Status", "Type", "BarCode",
        "GroupName", "CountryOrigin", "CommodityCode", "HasLots",
        "Qty", "InterimAvailable",
        "GrossAmount", "TaxRate", "Currency", "Discount",
        "SupplierName", "SupplierProductCode", "SupplierProductName",
        "PurchasePrice", "PurchasePriceCurrency", "PurchasePriceIncludeTax", "SupplierIsDefault",
        "CreatedUTC", "UpdatedUTC",
    ]
    ws.update("A1:" + rowcol_to_a1(1, len(headers)), [headers])
    return headers


def build_itemid_map(ws, headers: List[str]) -> Tuple[Dict[str, Tuple[int, List[str]]], int]:
    """
    Izveido map: ItemID -> (row_index, row_values)
    """
    if "ItemID" not in headers:
        raise RuntimeError("Sheetā nav 'ItemID' kolonnas")

    item_idx = headers.index("ItemID")  # 0-based
    all_rows = ws.get_all_values()

    mapping: Dict[str, Tuple[int, List[str]]] = {}
    for i in range(1, len(all_rows)):  # sākot ar 2. rindu
        row_index = i + 1  # 1-based
        row = all_rows[i]
        if len(row) <= item_idx:
            continue
        item_id = (row[item_idx] or "").strip()
        if not item_id:
            continue
        mapping[item_id] = (row_index, row)

    return mapping, item_idx


def make_row_from_headers(item: Dict, headers: List[str]) -> List[str]:
    """
    Pārvērš produkta dict -> rindu pēc headers secības.
    """
    row = []
    for h in headers:
        row.append(item.get(h, "") or "")
    return row


def get_or_create_updates_sheet(sh, title: str = "Product updates"):
    """
    Atrod vai izveido 'Product updates' sheet.
    Formāts:
    TimestampRiga | ItemID | Code | Name | FieldName | OldValue | NewValue
    """
    try:
        ws = sh.worksheet(title)
    except gspread.WorksheetNotFound:
        ws = sh.add_worksheet(title=title, rows=2000, cols=7)
        header_row = ["TimestampRiga", "ItemID", "Code", "Name", "FieldName", "OldValue", "NewValue"]
        ws.update("A1:" + rowcol_to_a1(1, len(header_row)), [header_row])
        return ws

    existing_headers = ws.row_values(1)
    if not existing_headers:
        header_row = ["TimestampRiga", "ItemID", "Code", "Name", "FieldName", "OldValue", "NewValue"]
        ws.update("A1:" + rowcol_to_a1(1, len(header_row)), [header_row])

    return ws


# =========================
# Salīdzināšanas normalizācija
# =========================

def normalize_for_compare(val: str) -> str:
    """
    Normalizē vērtību salīdzināšanai.
    - Apgriež atstarpes
    - Ja izskatās pēc skaitļa (ar . vai ,), pārveido uz kanonisku formu:
      * 13,00 -> 13
      * 0.000 -> 0
      * 21.5 -> 21.5
    - Ja nav skaitlis, salīdzina kā plain tekstu (bez ārējām atstarpēm)
    """
    if val is None:
        return ""
    s = str(val).strip()
    if s == "":
        return ""

    # Mēģinām interpretēt kā skaitli
    # (gan 13.00, gan 13,00 -> 13)
    candidate = s.replace(" ", "").replace(",", ".")
    try:
        num = float(candidate)
        if num.is_integer():
            return str(int(num))
        # līdz 4 zīmēm aiz komata, bez liekiem nullēm
        return f"{num:.4f}".rstrip("0").rstrip(".")
    except ValueError:
        # Nav skaitlis – salīdzinām tekstuāli
        return s


# =========================
# Flask API
# =========================

@app.get("/")
def health():
    return jsonify({
        "ok": True,
        "service": "paytraq-products-updated-today",
        "usage": [
            "GET /products-updated-today",
            "GET /sync-updated-products-to-sheet?spreadsheet_id=...&worksheet=Products_FULL"
        ]
    }), 200


@app.get("/products-updated-today")
def products_updated_today():
    """
    Parāda visus produktus, kuriem UpdatedUTC ir šodien (pēc Europe/Riga).
    """
    if not PAYTRAQ_KEY or not PAYTRAQ_TOKEN:
        return jsonify({"ok": False, "error": "Missing PAYTRAQ_API_KEY or PAYTRAQ_API_TOKEN"}), 400

    want_suppliers = request.args.get("suppliers", "0").lower() in ("1", "true", "yes")
    start_iso, end_iso = riga_today_start_end_utc()

    items_all, debug = fetch_all_products(want_suppliers=want_suppliers)
    if items_all is None:
        return jsonify({
            "ok": False,
            "error": "Fetch failed",
            "debug": debug
        }), 502

    today_updated = []
    for p in items_all:
        norm = normalize_product(p, want_suppliers)
        ts = norm["UpdatedUTC"]
        if ts and (start_iso <= ts <= end_iso):
            today_updated.append(norm)

    return jsonify({
        "ok": True,
        "window_utc": {"start": start_iso, "end": end_iso},
        "count": len(today_updated),
        "products": today_updated,
        "debug": debug[-10:]
    }), 200


@app.get("/sync-updated-products-to-sheet")
def sync_updated_products_to_sheet():
    """
    1) Atrod visus šodien UPDATED produktus PayTraq
    2) Google Sheetā (pēc ItemID) pārraksta rindas galvenajā lapā (Products_FULL)
    3) 'Product updates' lapā pieraksta PA VIENAI RINDAI par katru lauku,
       kas PATIESSI mainījies (ignorē laukus, kur vecā un jaunā vērtība
       pēc normalizācijas sakrīt):
       TimestampRiga | ItemID | Code | Name | FieldName | OldValue | NewValue
    """
    if not PAYTRAQ_KEY or not PAYTRAQ_TOKEN:
        return jsonify({"ok": False, "error": "Missing PAYTRAQ_API_KEY or PAYTRAQ_API_TOKEN"}), 400
    if not GOOGLE_SA_JSON:
        return jsonify({"ok": False, "error": "Missing GOOGLE_SA_JSON env var"}), 400

    spreadsheet_id = request.args.get("spreadsheet_id", "").strip()
    worksheet_name = request.args.get("worksheet", "Products_FULL").strip()
    log_worksheet_name = request.args.get("log_worksheet", "Product updates").strip()
    want_suppliers = request.args.get("suppliers", "0").lower() in ("1", "true", "yes")

    if not spreadsheet_id:
        return jsonify({"ok": False, "error": "Missing spreadsheet_id param"}), 400

    # 1) Paņemam visus produktus un filtrējam pēc UpdatedUTC šodien
    start_iso, end_iso = riga_today_start_end_utc()

    items_all, debug = fetch_all_products(want_suppliers=want_suppliers)
    if items_all is None:
        return jsonify({
            "ok": False,
            "error": "Fetch failed from PayTraq",
            "debug": debug
        }), 502

    updated_today = []
    for p in items_all:
        norm = normalize_product(p, want_suppliers)
        ts = norm["UpdatedUTC"]
        if ts and (start_iso <= ts <= end_iso):
            updated_today.append(norm)

    # 2) Google Sheets sagatavošana (galvenais sheet + Product updates)
    try:
        gc = get_gspread_client()
        sh = gc.open_by_key(spreadsheet_id)

        ws = sh.worksheet(worksheet_name)
        headers = ensure_headers(ws)
        item_map, item_idx = build_itemid_map(ws, headers)

        updates_ws = get_or_create_updates_sheet(sh, title=log_worksheet_name)
    except Exception as e:
        return jsonify({
            "ok": False,
            "error": f"Sheets access error: {repr(e)}"
        }), 500

    if not updated_today:
        return jsonify({
            "ok": True,
            "message": "Šodien PayTraq nav neviena updated produkta.",
            "counts": {"updated_today": 0, "updated_rows": 0, "not_found": 0},
            "sheet": {
                "spreadsheet_id": spreadsheet_id,
                "worksheet": worksheet_name,
                "log_worksheet": log_worksheet_name,
            },
            "updated_products": [],
            "not_found": [],
            "window_utc": {"start": start_iso, "end": end_iso},
            "debug": debug[-10:]
        }), 200

    updates_info = []
    not_found = []
    update_requests = []

    # Rīgas timestamp logam
    riga = tz.gettz("Europe/Riga")
    now_riga = dt.datetime.now(riga)
    ts_riga_str = now_riga.strftime("%Y-%m-%d %H:%M:%S")

    # Product updates – nākamā brīvā rinda
    existing_updates_rows = updates_ws.get_all_values()
    next_updates_row = len(existing_updates_rows) + 1  # 1-based

    for it in updated_today:
        item_id = (it.get("ItemID") or "").strip()
        if not item_id:
            continue

        if item_id not in item_map:
            not_found.append({
                "ItemID": item_id,
                "Code": it.get("Code", ""),
                "Name": it.get("Name", "")
            })
            continue

        row_index, old_row = item_map[item_id]
        new_row = make_row_from_headers(it, headers)

        changed_fields: Dict[str, Dict[str, str]] = {}
        max_len = max(len(old_row), len(new_row))

        for idx in range(max_len):
            old_val = old_row[idx] if idx < len(old_row) else ""
            new_val = new_row[idx] if idx < len(new_row) else ""

            old_norm = normalize_for_compare(old_val)
            new_norm = normalize_for_compare(new_val)

            # Ja pēc normalizācijas vienādi -> IGNORĒJAM
            if old_norm == new_norm:
                continue

            field_name = headers[idx] if idx < len(headers) else f"COL_{idx+1}"
            changed_fields[field_name] = {
                "old": old_val,
                "new": new_val,
            }

        if not changed_fields:
            # Reāli nekā nav mainīts – neliekam ne galvenajā, ne atskaitē
            continue

        # galvenā lapa: rindu pārrakstīšana
        update_requests.append((row_index, new_row))

        # info API atbildē
        updates_info.append({
            "ItemID": item_id,
            "Code": it.get("Code", ""),
            "Name": it.get("Name", ""),
            "changed_fields": changed_fields
        })

        # Product updates sheet: pa vienai rindai katram mainītajam laukam
        updates_rows_batch = []
        for field_name, diff in changed_fields.items():
            old_val = diff.get("old", "")
            new_val = diff.get("new", "")
            updates_rows_batch.append([
                ts_riga_str,
                item_id,
                it.get("Code", ""),
                it.get("Name", ""),
                field_name,
                old_val,
                new_val,
            ])

        if updates_rows_batch:
            start_cell = rowcol_to_a1(next_updates_row, 1)
            end_cell = rowcol_to_a1(next_updates_row + len(updates_rows_batch) - 1, 7)
            updates_ws.update(
                f"{start_cell}:{end_cell}",
                updates_rows_batch,
                value_input_option="USER_ENTERED"
            )
            next_updates_row += len(updates_rows_batch)

    # 3) Pārrakstām rindas sheetā (galvenajā lapā)
    for row_index, new_row in update_requests:
        start_cell = rowcol_to_a1(row_index, 1)
        end_cell = rowcol_to_a1(row_index, len(headers))
        ws.update(f"{start_cell}:{end_cell}", [new_row], value_input_option="USER_ENTERED")

    return jsonify({
        "ok": True,
        "sheet": {
            "spreadsheet_id": spreadsheet_id,
            "worksheet": worksheet_name,
            "log_worksheet": log_worksheet_name,
        },
        "window_utc": {"start": start_iso, "end": end_iso},
        "counts": {
            "updated_today": len(updated_today),
            "updated_rows": len(update_requests),
            "not_found": len(not_found)
        },
        "updated_products": updates_info,
        "not_found": not_found,
        "debug": debug[-10:]
    }), 200


# =========================
# Palaišana lokāli
# =========================
if __name__ == "__main__":
    port = int(os.environ.get("PORT", "8080"))
    app.run(host="0.0.0.0", port=port)
