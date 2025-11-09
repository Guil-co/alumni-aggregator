# scraper_api_paged.py — FINAL (ESSEC API v2, schéma confirmé)
import os, json, requests
import pandas as pd

BASE = "https://www.essecalumni.com"
URL  = (BASE + "/api/v2/public/agenda/occurrence/visitor/occurrence"
        "?language=auto&published=1&order[begin_at]=asc&when=upcoming&properties[0]=group&limit=12")

HEADERS = {
    "Accept": "application/json, text/plain, */*",
    "User-Agent": "Mozilla/5.0",
    "Referer": BASE + "/fr/agenda/upcoming",
}

def fetch_all(url: str):
    """Suit _links.next.href (HAL) et concatène toutes les pages."""
    all_items = []
    page = 0
    while url and page < 100:
        page += 1
        full = url if url.startswith("http") else (BASE + url)
        print(f"[PAGE {page}] {full}")
        r = requests.get(full, headers=HEADERS, timeout=20)
        r.raise_for_status()
        data = r.json()

        items = (data.get("_embedded") or {}).get("items", [])
        if page == 1 and items:
            print("↪︎ Clés (échantillon 1er item) :", sorted(items[0].keys()))
        print(f"  -> {len(items)} items")
        all_items.extend(items)

        url = ((data.get("_links") or {}).get("next") or {}).get("href")
    print(f"✅ Total récupéré: {len(all_items)}")
    return all_items

def normalize(items):
    """Mappe exactement le schéma constaté (begin_at/end_at, is_on_site, _embedded.address...)."""
    rows = []
    for ev in items:
        # Champs de base (snake_case)
        eid   = str(ev.get("id") or "")
        title = ev.get("title") or "(Sans titre)"
        start = ev.get("begin_at") or ev.get("beginAt") or ev.get("date")
        end   = ev.get("end_at")   or ev.get("endAt")
        url   = ev.get("web_url") or ev.get("url") or (f"{BASE}/fr/calendar/index/index?id={eid}" if eid else None)

        # Détermination du mode
        is_on_site = bool(ev.get("is_on_site"))
        is_webinar = bool(ev.get("is_webinar"))
        is_online  = (not is_on_site) or is_webinar  # logique confirmée par ton extrait

        # Adresse (présente seulement si sur site)
        addr = ((ev.get("_embedded") or {}).get("address")) or {}
        # addr a ce format: {"venue","address","address_2","city","zip","country_iso"}
        venue   = (addr.get("venue") or "").strip()
        line1   = (addr.get("address") or "").strip()
        line2   = (addr.get("address_2") or "").strip()
        zipc    = (addr.get("zip") or "").strip()
        city    = (addr.get("city") or "").strip()
        country = (addr.get("country_iso") or "").strip()

        if is_online:
            location = "En ligne"
            # city peut rester vide si en ligne
        else:
            parts = [p for p in [venue, line1, line2, zipc, city, country] if p]
            location = ", ".join(parts)

        # Description / image
        description = (ev.get("description") or "").strip()
        image_url = ev.get("cover") or ev.get("thumbnail")  # URLs complètes déjà présentes

        rows.append({
            "school": "ESSEC Alumni",
            "title": title,
            "start": start,
            "end": end,
            "url": url,
            "location": location,     # "En ligne" si online, sinon adresse concaténée
            "city": city or None,     # utile pour filtre Ville
            "isOnline": is_online,
            "description": description,
            "imageUrl": image_url,
        })
    return rows

def main():
    items = fetch_all(URL)
    events = normalize(items)

    os.makedirs("output", exist_ok=True)
    df = pd.DataFrame(events)
    if "start" in df.columns:
        df["start"] = pd.to_datetime(df["start"], errors="coerce")
        df = df.sort_values("start", na_position="last")
    df.to_csv("output/events.csv", index=False)

    safe = json.loads(df.to_json(orient="records", date_format="iso"))
    with open("output/events.json", "w", encoding="utf-8") as f:
        json.dump(safe, f, ensure_ascii=False, indent=2)

    print("✅ Export : output/events.csv, output/events.json")

if __name__ == "__main__":
    main()
