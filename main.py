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


def fetch_all_products(want_suppliers: bool = False,
                       max_pages: int = 500,
                       page_sleep: float = 0.4) -> Tuple[List[Dict], List[str]]:
    """
    Lapo cauri /products un savāc VISUS produktus.
    (tieši tāda pieeja kā tavā esošajā servisā, tikai vienkāršāka)
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
        norm = normalize_product_simple(p)
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


# =========================
# Palaišana lokāli
# =========================
if __name__ == "__main__":
    port = int(os.environ.get("PORT", "8080"))
    app.run(host="0.0.0.0", port=port)
