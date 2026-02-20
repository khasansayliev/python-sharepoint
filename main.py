#!/usr/bin/env python3
"""
SharePoint ETL — sodda versiya
NTLM auth bilan barcha listlarni JSON ga yuklaydi
"""

import json
import os
from pathlib import Path
from datetime import datetime

import requests
from requests_ntlm import HttpNtlmAuth
from dotenv import load_dotenv

load_dotenv()

# ══════════════════════════════════════════════════════════════════
# SOZLAMALAR
# ══════════════════════════════════════════════════════════════════

SITE_URL = os.environ["SHAREPOINT_SITE_URL"]  # https://contoso.sharepoint.com/sites/MySite
USERNAME = os.environ["SP_USERNAME"]           # DOMAIN\user yoki user@contoso.com
PASSWORD = os.environ["SP_PASSWORD"]
OUTPUT_DIR = Path(os.getenv("OUTPUT_DIR", "output"))

# System listlarni o'tkazib yuborish
SKIP_TEMPLATES = {100, 101, 102, 109, 110, 1100, 3100}


# ══════════════════════════════════════════════════════════════════
# ASOSIY KOD
# ══════════════════════════════════════════════════════════════════

def get_session():
    """NTLM session yaratish"""
    s = requests.Session()
    s.auth = HttpNtlmAuth(USERNAME, PASSWORD)
    s.headers.update({"Accept": "application/json;odata=verbose"})
    return s


def api_url(path):
    """REST API URL yasash"""
    return f"{SITE_URL.rstrip('/')}/_api{path}"


def get_all_items(session, url):
    """Pagination bilan barcha itemlarni olish"""
    results = []
    while url:
        resp = session.get(url, timeout=60)
        resp.raise_for_status()
        data = resp.json().get("d", {})
        results.extend(data.get("results", []))
        url = data.get("__next")
    return results


def get_lists(session):
    """Barcha foydalanuvchi listlarini olish"""
    url = api_url("/web/lists?$select=Id,Title,ItemCount&$filter=Hidden eq false")
    all_lists = get_all_items(session, url)
    
    # System listlarni filtrlash
    user_lists = [
        lst for lst in all_lists
        if lst.get("BaseTemplate", 0) not in SKIP_TEMPLATES
    ]
    return user_lists


def get_list_items(session, list_id):
    """List itemlarini olish"""
    url = api_url(
        f"/web/lists(guid'{list_id}')/items"
        "?$top=1000&$select=*,FieldValuesAsText&$expand=FieldValuesAsText"
    )
    items = get_all_items(session, url)
    
    # FieldValuesAsText dan ma'lumot olish
    result = []
    for item in items:
        fields = item.get("FieldValuesAsText", {})
        fields["_id"] = item.get("Id")
        fields["_created"] = item.get("Created")
        fields["_modified"] = item.get("Modified")
        result.append(fields)
    
    return result


def save_to_json(data, filename):
    """JSON faylga saqlash"""
    OUTPUT_DIR.mkdir(exist_ok=True)
    filepath = OUTPUT_DIR / filename
    with open(filepath, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    print(f"  ✅ Saqlandi: {filepath}")


def main():
    """Asosiy ETL jarayoni"""
    print(f"🚀 SharePoint ETL boshlandi...")
    print(f"📍 Site: {SITE_URL}")
    print(f"👤 User: {USERNAME}")
    
    session = get_session()
    
    # Test ulanish
    try:
        resp = session.get(api_url("/web?$select=Title"), timeout=10)
        resp.raise_for_status()
        site_title = resp.json()["d"]["Title"]
        print(f"✅ Ulandi: {site_title}\n")
    except Exception as e:
        print(f"❌ Ulanish xatosi: {e}")
        return
    
    # Barcha listlar
    print("📋 Listlarni yuklamoqda...")
    lists = get_lists(session)
    print(f"   {len(lists)} ta list topildi\n")
    
    # Har bir list
    total_items = 0
    for lst in lists:
        list_id = lst["Id"]
        list_name = lst["Title"]
        item_count = lst.get("ItemCount", 0)
        
        print(f"📦 {list_name} ({item_count} items)")
        
        try:
            items = get_list_items(session, list_id)
            
            # JSON ga saqlash
            filename = f"{list_name.lower().replace(' ', '_')}.json"
            save_to_json(items, filename)
            
            total_items += len(items)
            
        except Exception as e:
            print(f"  ❌ Xato: {e}")
    
    # Xulosa
    print(f"\n✅ Tugadi!")
    print(f"   Listlar: {len(lists)}")
    print(f"   Itemlar: {total_items}")
    print(f"   Papka: {OUTPUT_DIR.absolute()}")


if __name__ == "__main__":
    main()
