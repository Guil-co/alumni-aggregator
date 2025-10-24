# multi_schools.py
import os, json, time, re
import requests
import pandas as pd
from urllib.parse import urlencode, urlparse, parse_qs, urlunparse
from datetime import datetime, timedelta, timezone

# ---------- CONFIG ----------
HEADERS = {
    "Accept": "application/json, text/plain, */*",
    "User-Agent": "Mozilla/5.0",
}

SITES = [
    {
        "school": "ESSEC Alumni",
        "base": "https://www.essecalumni.com",
        "url":  "https://www.essecalumni.com/api/v2/public/agenda/occurrence/visitor/occurrence?language=auto&published=1&order[begin_at]=asc&when=upcoming&properties[0]=group&limit=50",
        "type": "api_v2_hal",
    },
    {
        "school": "HEC Alumni",
        "base": "https://www.hecalumni.fr",
        "url":  "https://www.hecalumni.fr/api/v2/public/agenda/occurrence/visitor/occurrence?language=auto&published=1&order[begin_at]=asc&when=upcoming&properties[0]=group&limit=12",
        "type": "api_v2_hal",
    },
    {
        "school": "Mines Paris Alumni",
        "base": "https://mines-paris.org",
        "url":  "https://mines-paris.org/api/v2/public/agenda/occurrence/visitor/occurrence?language=auto&published=1&order[begin_at]=asc&when=upcoming&properties[0]=group&properties[1]=streamer&limit=50",
        "type": "api_v2_hal",
    },
    {
        "school": "Arts et Métiers Alumni",
        "base": "https://www.arts-et-metiers.asso.fr",
        "url":  "https://www.arts-et-metiers.asso.fr/events.json",  # le fetcher injecte les params
        "type": "arts_json",
    },
    {
    "school": "AX Polytechnique Alumni",
    "base": "https://ax.polytechnique.org",
    "url":  "https://ax.polytechnique.org/api/v2/public/agenda/occurrence/visitor/occurrence?language=auto&published=1&order[begin_at]=asc&when=upcoming&properties[0]=group&limit=50",
    "type": "api_v2_hal",
    },
    {
    "school": "Dauphine Alumni",
    "base": "https://www.dauphine-alumni.org",
    "url":  "https://www.dauphine-alumni.org/api/v2/public/agenda/occurrence/visitor/occurrence?language=auto&published=1&order[begin_at]=asc&when=upcoming&properties[0]=group&limit=50",
    "type": "api_v2_hal",
    },
    {
    "school": "CentraleSupélec Alumni",
    "base": "https://association.centralesupelec-alumni.com",
    "url":  "https://association.centralesupelec-alumni.com/api/v2/public/agenda/occurrence/visitor/occurrence?language=auto&published=1&order[begin_at]=asc&when=upcoming&properties[0]=group&properties[1]=streamer&limit=50",
    "type": "api_v2_hal",
    },

]

# ---------- HELPERS ----------
def _safe_get(d, *keys, default=None):
    for k in keys:
        if isinstance(d, dict) and k in d and d[k] not in (None, ""):
            return d[k]
    return default

def _concat_address(*parts):
    return ", ".join([str(x).strip() for x in parts if x])

def now_ms():
    return int(time.time() * 1000)

def iso_utc(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

def to_iso(x):
    """
    Convertit n'importe quoi en ISO UTC (ou None).
    Gère: int/str timestamps (ms/s), ISO, formats libres.
    """
    if x is None or x == "":
        return None
    if isinstance(x, (int, float)):
        try:
            unit = "ms" if x > 1e11 else "s"
            return pd.to_datetime(x, unit=unit, utc=True).isoformat()
        except Exception:
            return None
    s = str(x).strip()
    if s.isdigit():
        try:
            n = int(s)
            unit = "ms" if n > 1e11 else "s"
            return pd.to_datetime(n, unit=unit, utc=True).isoformat()
        except Exception:
            return None
    try:
        return pd.to_datetime(s, utc=True).isoformat()
    except Exception:
        return None

# ---------- FETCHERS (ESSEC / HEC / MINES) ----------
def fetch_all_api_v2_hal(base, url):
    """Suit _links.next.href (relatives ou absolues) et renvoie la liste brute des items."""
    all_items, page = [], 0
    while url and page < 200:
        page += 1
        full = url if url.startswith("http") else (base + url)
        print(f"[{base}] PAGE {page} → {full}")
        r = requests.get(full, headers=HEADERS, timeout=30)
        r.raise_for_status()
        data = r.json()
        items = (data.get("_embedded") or {}).get("items", [])
        print(f"  -> {len(items)} items")
        all_items.extend(items)
        url = ((data.get("_links") or {}).get("next") or {}).get("href")
        time.sleep(0.05)
    return all_items

# ---------- FETCHER (ARTS & MÉTIERS) ----------
def fetch_all_arts_json(base, url):
    """
    Arts & Métiers : liste paginée. Le JSON renvoie généralement:
    { "total_items": N, "events": [ {...}, ... ] }
    On injecte des paramètres larges et on boucle sur ?page=.
    """
    pr = urlparse(url)
    qs = parse_qs(pr.query)
    qs["include_network_events"] = ["true"]
    qs["query[pinned_on_index_page]"] = ["false"]
    qs["query[order]"] = ["asc"]
    qs["per_page"] = ["100"]
    # depuis J-30, en ms
    qs["query[gte_start_date]"] = [str(now_ms() - 30*24*3600*1000)]

    all_items, page = [], 0
    while True:
        page += 1
        qs["page"] = [str(page)]
        nqs = urlencode({k: v[0] if isinstance(v, list) and len(v) == 1 else v for k, v in qs.items()}, doseq=True)
        full = urlunparse(pr._replace(query=nqs))
        print(f"[A&M] PAGE {page} → {full}")
        r = requests.get(full, headers={**HEADERS, "Referer": f"{base}/events"}, timeout=30)
        if r.status_code == 400:
            print("[A&M] 400 – stop.")
            break
        r.raise_for_status()
        data = r.json()

        # -> la clé correcte est "events" (sinon liste directe)
        items = data.get("events") if isinstance(data, dict) else (data if isinstance(data, list) else [])
        if page == 1:
            os.makedirs("output/debug", exist_ok=True)
            with open("output/debug/arts_raw_p1.json", "w", encoding="utf-8") as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
            with open("output/debug/arts_items_p1.json", "w", encoding="utf-8") as f:
                json.dump(items[:3], f, ensure_ascii=False, indent=2)
            if items:
                print("  (A&M) clés 1er item :", sorted(items[0].keys()))
        print(f"  -> {len(items)} items")
        if not items:
            break

        all_items.extend(items)
        # fin probable si moins que per_page
        if len(items) < int(qs["per_page"][0]):
            break
        time.sleep(0.05)

    print(f"[A&M] total items récupérés: {len(all_items)}")
    return all_items

# ---------- NORMALISATION ----------
def normalize_api_v2_hal(school, base, ev):
    eid   = str(_safe_get(ev, "id", default="") or "")
    title = _safe_get(ev, "title", default="(Sans titre)")
    start_raw = _safe_get(ev, "begin_at", "beginAt", "date")
    end_raw   = _safe_get(ev, "end_at", "endAt")
    start = to_iso(start_raw)
    end   = to_iso(end_raw)
    url   = _safe_get(ev, "web_url", "url", default=(f"{base}/fr/calendar/index/index?id={eid}" if eid else None))

    is_on_site = bool(_safe_get(ev, "is_on_site", default=False))
    is_webinar = bool(_safe_get(ev, "is_webinar", default=False))
    is_online  = (not is_on_site) or is_webinar

    addr = (ev.get("_embedded") or {}).get("address") or {}
    venue   = (addr.get("venue") or "").strip()
    line1   = (addr.get("address") or "").strip()
    line2   = (addr.get("address_2") or "").strip()
    zipc    = (addr.get("zip") or "").strip()
    city    = (addr.get("city") or "").strip()
    country = (addr.get("country_iso") or "").strip()

    location = "En ligne" if is_online else _concat_address(venue, line1, line2, zipc, city, country)

    description = (_safe_get(ev, "description", default="") or "").strip()
    image_url = _safe_get(ev, "cover", "thumbnail")

    return {
        "school": school,
        "title": title,
        "start": start,
        "end": end,
        "url": url,
        "location": location,
        "city": city or None,
        "isOnline": bool(is_online),
        "description": description,
        "imageUrl": image_url,
    }

def _city_from_address(addr: str):
    if not addr:
        return None
    # ex: "..., 75008 Paris, France" → Paris
    m = re.search(r"\b\d{4,5}\s+([A-Za-zÀ-ÖØ-öø-ÿ' -]+)", addr)
    if m:
        return m.group(1).strip()
    parts = [p.strip() for p in addr.split(",") if p.strip()]
    if len(parts) >= 2:
        return parts[-2]
    return None

def normalize_arts_json(school, base, ev):
    # ---- titre / url ----
    title = _safe_get(ev, "title", "name", default="(Sans titre)")
    url = (_safe_get(ev, "web_url", "url", "full_url")
           or (f"{base}/events/{_safe_get(ev,'slug','id',default='')}" if _safe_get(ev,'slug','id') else None))

    # ---- dates (A&M) ----
    start = to_iso(_safe_get(ev, "start_date", "begin_at", "start_at"))
    end   = to_iso(_safe_get(ev, "end_date", "end_at", "end_time"))

    # ---- lieu / online ----
    loc_obj = ev.get("locations")
    if isinstance(loc_obj, dict):
        location = (loc_obj.get("address") or "").strip()
        city = _city_from_address(location)
        is_online = False
    else:
        # locations == null => en ligne
        location = "En ligne"
        city = None
        is_online = True

    # ---- description / image ----
    description = (_safe_get(ev, "description", "content", default="") or "").strip()
    img = _safe_get(ev, "cover", "thumbnail", "image", "picture", "cover_url", "image_url")
    image_url = img.get("url") if isinstance(img, dict) else (img if isinstance(img, str) else None)

    return {
        "school": school,
        "title": title,
        "start": start,
        "end": end,
        "url": url,
        "location": location,  # "En ligne" si locations == null
        "city": city,
        "isOnline": bool(is_online),
        "description": description,
        "imageUrl": image_url,
    }

# ---------- PIPELINE ----------
def main():
    all_rows = []
    for site in SITES:
        school, base, url, stype = site["school"], site["base"], site["url"], site["type"]
        try:
            if stype == "api_v2_hal":
                items = fetch_all_api_v2_hal(base, url)
                rows = [normalize_api_v2_hal(school, base, ev) for ev in items]
            elif stype == "arts_json":
                items = fetch_all_arts_json(base, url)
                rows = [normalize_arts_json(school, base, ev) for ev in items]
            else:
                print(f"[WARN] Type inconnu pour {school} → ignoré")
                rows = []
        except Exception as e:
            print(f"[ERROR] {school}: {e}")
            rows = []
        print(f"✔ {school}: {len(rows)} événements")
        all_rows.extend(rows)

    # exports
    os.makedirs("output", exist_ok=True)
    df = pd.DataFrame(all_rows)
    if "start" in df.columns:
        df["start"] = pd.to_datetime(df["start"], errors="coerce")
        df = df.sort_values(["start","school"], na_position="last")
    df.to_csv("output/events.csv", index=False)

    safe = json.loads(df.to_json(orient="records", date_format="iso"))
    with open("output/events.json", "w", encoding="utf-8") as f:
        json.dump(safe, f, ensure_ascii=False, indent=2)

    print(f"✅ Export fusionné : output/events.csv, output/events.json")
    print("ℹ️ Debug A&M : output/debug/arts_raw_p1.json & arts_items_p1.json")

if __name__ == "__main__":
    main()
