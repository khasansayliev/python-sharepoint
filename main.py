#!/usr/bin/env python3
"""
SharePoint ETL — Production Safe Version
Barcha user listlarni to‘liq JSON ga yuklaydi
"""

import json
import os
from pathlib import Path
import requests
from requests_ntlm import HttpNtlmAuth
from dotenv import load_dotenv

load_dotenv()

# ══════════════════════════════════════════════════════════════════
# SOZLAMALAR
# ══════════════════════════════════════════════════════════════════

SITE_URL = os.environ["SHAREPOINT_SITE_URL"]
USERNAME = os.environ["SP_USERNAME"]
PASSWORD = os.environ["SP_PASSWORD"]
OUTPUT_DIR = Path(os.getenv("OUTPUT_DIR", "output"))

# Faqat biznes listlar (Custom List + Document Library)
ALLOWED_TEMPLATES = {100, 101}


# ══════════════════════════════════════════════════════════════════
# SESSION
# ══════════════════════════════════════════════════════════════════

def get_session():
    s = requests.Session()
    s.auth = HttpNtlmAuth(USERNAME, PASSWORD)
    s.headers.update({"Accept": "application/json;odata=verbose"})
    return s


def api_url(path):
    return f"{SITE_URL.rstrip('/')}/_api{path}"


def get_all_items(session, url):
    results = []

    while url:
        resp = session.get(url, timeout=60)
        resp.raise_for_status()

        data = resp.json().get("d", {})
        results.extend(data.get("results", []))

        url = data.get("__next")

    return results


# ══════════════════════════════════════════════════════════════════
# LISTLARNI OLISH
# ══════════════════════════════════════════════════════════════════

def get_lists(session):
    url = api_url(
        "/web/lists?"
        "$select=Id,Title,ItemCount,BaseTemplate,Hidden,IsCatalog"
    )

    all_lists = get_all_items(session, url)

    user_lists = [
        lst for lst in all_lists
        if not lst.get("Hidden", False)
        and not lst.get("IsCatalog", False)
        and lst.get("BaseTemplate") in ALLOWED_TEMPLATES
    ]

    return user_lists


# ══════════════════════════════════════════════════════════════════
# ITEMLARNI OLISH
# ══════════════════════════════════════════════════════════════════

def get_list_items(session, list_id):
    """
    To‘liq raw itemlarni oladi.
    select=* ishlatmaymiz.
    """
    url = api_url(
        f"/web/lists(guid'{list_id}')/items?$top=1000"
    )

    return get_all_items(session, url)


# ══════════════════════════════════════════════════════════════════
# JSON SAQLASH
# ══════════════════════════════════════════════════════════════════

def save_to_json(data, filename):
    OUTPUT_DIR.mkdir(exist_ok=True)
    filepath = OUTPUT_DIR / filename

    with open(filepath, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

    print(f"  ✅ Saqlandi: {filepath}")


# ══════════════════════════════════════════════════════════════════
# MAIN
# ══════════════════════════════════════════════════════════════════

def main():
    print("🚀 SharePoint ETL boshlandi...")
    print(f"📍 Site: {SITE_URL}")
    print(f"👤 User: {USERNAME}")

    session = get_session()

    # Test connection
    try:
        resp = session.get(api_url("/web?$select=Title"), timeout=10)
        resp.raise_for_status()
        site_title = resp.json()["d"]["Title"]
        print(f"✅ Ulandi: {site_title}\n")
    except Exception as e:
        print(f"❌ Ulanish xatosi: {e}")
        return

    print("📋 Listlarni yuklamoqda...")
    lists = get_lists(session)
    print(f"   {len(lists)} ta list topildi\n")

    total_items = 0

    for lst in lists:
        list_id = lst["Id"]
        list_name = lst["Title"]
        item_count = lst.get("ItemCount", 0)

        print(f"📦 {list_name} ({item_count} items)")

        try:
            items = get_list_items(session, list_id)

            filename = f"{list_name.lower().replace(' ', '_')}.json"
            save_to_json(items, filename)

            total_items += len(items)

        except Exception as e:
            print(f"  ❌ Xato: {e}")

    print("\n✅ Tugadi!")
    print(f"   Listlar: {len(lists)}")
    print(f"   Itemlar: {total_items}")
    print(f"   Papka: {OUTPUT_DIR.absolute()}")


if __name__ == "__main__":
    main()
