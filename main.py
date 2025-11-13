import os
import time
import datetime as dt
from typing import List, Dict, Tuple

import requests
import xmltodict
from dateutil import tz
from flask import Flask, jsonify, request

# =========================
# ENV konfigurācija
# =========================

PAYTRAQ_KEY   = os.environ.get("PAYTRAQ_API_KEY", "")
PAYTRAQ_TOKEN = os.environ.get("PAYTRAQ_API_TOKEN", "")

BASE_URL = "https://go.paytraq.com/api"
app = Flask(__name__)


# =========================
# Palīgfunkcijas
# =========================

def riga_today_start_utc_iso() -> str:
    """
    Atgriež šodienas sākumu (00:00:00) Rīgā, pārtaisītu uz UTC ISO formātu.
    """
    riga = tz.gettz("Europe/Riga")
    now_riga = dt.datetime.now(riga)
    start_riga = dt.datetime(now_riga.year, now_riga.month, now_riga.day, 0, 0, 0, tzinfo=riga)
    start_utc = start_riga.astimezone(tz.UTC)
    return start_utc.strftime("%Y-%m-%dT%H:%M:%SZ")


def paytraq_get(path: str, params: Dict = None, timeout: int = 30):
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


def parse_products_xml(xml_text: str):
    """
    Atgriež produktu sarakstu no XML (<Products><Product>..)
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


def normalize_product_simple(p: Dict) -> Dict:
    """
    Vienkāršoti lauki, lai Chrome var normāli nolasīt.
    """
    stamps = (p.get("TimeStamps") or {})
    return {
        "ItemID": (p.get("ItemID") or "").strip(),
        "Code": (p.get("Code") or "").strip(),
        "Name": (p.get("Name") or "").strip(),
        "Status": (p.get("Status") or "").strip(),
        "GroupName": ((p.get("Group") or {}).get("GroupName") or "").strip(),
        "CreatedUTC": (stamps.get("Created") or "").strip(),
        "UpdatedUTC": (stamps.get("Updated") or "").strip(),
    }


def fetch_products_updated_after(updated_after_iso: str,
                                want_suppliers: bool = False,
                                max_pages: int = 200,
                                page_sleep: float = 0.3):
    """
    Lapo cauri PayTraq /products ar parametru updated_after.
    """
    collected = []
    debug = []
    page = 1

    base_params = {"updated_after": updated_after_iso}
    if want_suppliers:
        base_params["suppliers"] = "true"

    while page <= max_pages:
        params = dict(base_params)
        params["page"] = page

        status, text = paytraq_get("/products", params=params)
        debug.append(f"page={page} status={status}")

        if status >= 400:
            snippet = (text or "")[:200].replace("\n", " ")
            debug.append(f"HTTP_ERROR: {snippet}")
            break

        items = parse_products_xml(text)
        if not items:
            break

        collected.extend(items)

        page += 1
        time.sleep(page_sleep)

    return collected, debug


# =========================
# Flask API
# =========================

@app.get("/")
def health():
    return jsonify({
        "ok": True,
        "service": "paytraq-products-updated-today",
        "usage": "Open /products-updated-today in your browser"
    }), 200


@app.get("/products-updated-today")
def products_updated_today():
    """
    Chrome atvērams endpoints:
    Parāda visus produktus, kas šodien UPDATED PayTraq.
    """
    if not PAYTRAQ_KEY or not PAYTRAQ_TOKEN:
        return jsonify({"ok": False, "error": "Missing PAYTRAQ_API_KEY or PAYTRAQ_API_TOKEN"}), 400

    want_suppliers = request.args.get("suppliers", "0").lower() in ("1", "true", "yes")

    today_start_utc = riga_today_start_utc_iso()

    items_raw, debug = fetch_products_updated_after(today_start_utc, want_suppliers)

    normalized = [normalize_product_simple(p) for p in items_raw]

    return jsonify({
        "ok": True,
        "updated_after_utc": today_start_utc,
        "count": len(normalized),
        "products": normalized,
        "debug": debug[-10:]
    }), 200


# =========================
# Palaišana lokāli
# =========================
if __name__ == "__main__":
    port = int(os.environ.get("PORT", "8080"))
    app.run(host="0.0.0.0", port=port)
