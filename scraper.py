"""
Scraper d'événements alumni (multi-écoles) — Niveau simple
===========================================================

Ce starter kit fait 3 choses :
1) Tente d'abord de récupérer un flux iCal (.ics) public des pages d'agenda (quand il existe) — c'est le plus stable.
2) Sinon, bascule sur du scraping Selenium avec des sélecteurs CSS simples (+ pagination).
3) Exporte un CSV/JSON et génère une page HTML statique triée par date et filtrable par école.

Prérequis :
pip install selenium webdriver-manager beautifulsoup4 dateparser pandas icalendar lxml

Optionnel pour la visualisation locale :
pip install streamlit

Lancez :
python scraper.py
puis ouvrez « output/index.html » ou démarrez le mini viewer :
streamlit run viewer.py

Note importante : Les sélecteurs CSS varient selon les sites. Pour chaque école, renseignez/ajustez les sélecteurs dans SITE_CONFIGS ci‑dessous. 
Pour ESSEC Alumni, un exemple de sélecteurs plausibles est donné mais peut nécessiter un ajustement (inspecter la page).
"""

import os
import re
import csv
import json
import time
import uuid
import glob
import html
import shutil
import random
import string
import urllib.parse as urlparse
from dataclasses import dataclass, asdict
from typing import List, Optional, Dict

import requests
from bs4 import BeautifulSoup
from icalendar import Calendar
import pandas as pd
import dateparser

# ---- Selenium (fallback) ----
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options


@dataclass
class Event:
    school: str
    title: str
    start: Optional[str]
    end: Optional[str]
    url: Optional[str]
    location: Optional[str]
    raw_date_text: Optional[str]
    source: str


# ---------------------------------
# 1) Tentative iCal (recommandée)
# ---------------------------------

def find_ics_links(page_url: str) -> List[str]:
    """Cherche des liens .ics sur la page (link rel, anchors, etc.)."""
    try:
        r = requests.get(page_url, timeout=15)
        r.raise_for_status()
    except Exception:
        return []
    soup = BeautifulSoup(r.text, "lxml")

    links = set()
    # <link rel="alternate" type="text/calendar" href="...ics">
    for link in soup.find_all("link"):
        t = (link.get("type") or "").lower()
        href = link.get("href")
        if href and (".ics" in href.lower() or t == "text/calendar"):
            links.add(urlparse.urljoin(page_url, href))

    # <a href="...ics">
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if ".ics" in href.lower():
            links.add(urlparse.urljoin(page_url, href))

    return list(links)


def parse_ics(url_or_path: str, school: str, source: str) -> List[Event]:
    events = []
    try:
        if url_or_path.startswith("http"):
            data = requests.get(url_or_path, timeout=20).content
        else:
            with open(url_or_path, "rb") as f:
                data = f.read()
        cal = Calendar.from_ical(data)
        for component in cal.walk():
            if component.name == "VEVENT":
                title = str(component.get("summary", "")).strip()
                location = str(component.get("location", "")).strip() or None
                start = component.get("dtstart")
                end = component.get("dtend")
                url_field = component.get("url") or component.get("UID")
                ev = Event(
                    school=school,
                    title=title,
                    start=(start.dt.isoformat() if hasattr(start, 'dt') else None),
                    end=(end.dt.isoformat() if hasattr(end, 'dt') else None),
                    url=str(url_field) if url_field else None,
                    location=location,
                    raw_date_text=None,
                    source=source,
                )
                events.append(ev)
    except Exception:
        pass
    return events


# ---------------------------------
# 2) Scraping Selenium (fallback)
# ---------------------------------

@dataclass
class SiteConfig:
    school: str
    agenda_url: str
    # Pagination
    next_selector: Optional[str] = None  # CSS du bouton/lien « page suivante »
    max_pages: int = 5
    # Éléments d'un événement
    event_card_selector: str = ".event-card"  # à ajuster
    title_selector: str = ".event-card__title"
    date_selector: str = ".event-card__date"
    location_selector: Optional[str] = ".event-card__location"
    link_selector: str = "a"


# Exemple de configuration — à adapter en inspectant chaque site
SITE_CONFIGS: List[SiteConfig] = [
    SiteConfig(
        school="ESSEC Alumni",
        agenda_url="https://www.essecalumni.com/fr/agenda/upcoming",
        # Le site a plusieurs pages : souvent un lien rel=next, ou un bouton « Suivant ».
        next_selector="a[rel='next'], a.pagination-next, a[aria-label='Next']",
        # Sélecteurs plausibles (inspecter et ajuster si besoin)
        event_card_selector=".event, .event-card, .agenda-item",
        title_selector=".event-title, .event-card__title, h3, h2",
        date_selector=".event-date, .event-card__date, time",
        location_selector=".event-location, .event-card__location, .location",
        link_selector="a",
    ),
    # Ajoutez d'autres écoles ici…
]


def build_driver() -> webdriver.Chrome:
    opts = Options()
    opts.add_argument("--headless=new")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--window-size=1400,1000")
    service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service, options=opts)
    return driver


def scrape_with_selenium(site: SiteConfig) -> List[Event]:
    driver = build_driver()
    wait = WebDriverWait(driver, 10)
    events: List[Event] = []

    try:
        driver.get(site.agenda_url)
        time.sleep(1.5)

        for page in range(site.max_pages):
            # Attendre que des cards soient présentes
            try:
                wait.until(EC.presence_of_all_elements_located((By.CSS_SELECTOR, site.event_card_selector)))
            except Exception:
                # aucune card sur cette page ; on arrête la pagination
                break

            cards = driver.find_elements(By.CSS_SELECTOR, site.event_card_selector)
            for card in cards:
                try:
                    # Titre
                    title_el = None
                    for sel in site.title_selector.split(","):
                        try:
                            title_el = card.find_element(By.CSS_SELECTOR, sel.strip())
                            if title_el.text.strip():
                                break
                        except Exception:
                            continue
                    title = title_el.text.strip() if title_el else "(Sans titre)"

                    # Lien
                    link_el = None
                    try:
                        link_el = card.find_element(By.CSS_SELECTOR, site.link_selector)
                    except Exception:
                        pass
                    href = link_el.get_attribute("href") if link_el else None

                    # Date (texte brut + parsing FR)
                    date_el = None
                    for sel in site.date_selector.split(","):
                        try:
                            date_el = card.find_element(By.CSS_SELECTOR, sel.strip())
                            if date_el.text.strip():
                                break
                        except Exception:
                            continue
                    raw_date = date_el.text.strip() if date_el else None
                    parsed = dateparser.parse(raw_date, languages=["fr"]) if raw_date else None
                    start_iso = parsed.isoformat() if parsed else None

                    # Lieu
                    loc_text = None
                    if site.location_selector:
                        for sel in site.location_selector.split(","):
                            try:
                                loc_el = card.find_element(By.CSS_SELECTOR, sel.strip())
                                if loc_el.text.strip():
                                    loc_text = loc_el.text.strip()
                                    break
                            except Exception:
                                continue

                    events.append(Event(
                        school=site.school,
                        title=title,
                        start=start_iso,
                        end=None,
                        url=href,
                        location=loc_text,
                        raw_date_text=raw_date,
                        source=site.agenda_url,
                    ))
                except Exception:
                    continue

            # Pagination : cliquer sur Suivant si présent
            if site.next_selector:
                try:
                    nxt = None
                    for sel in site.next_selector.split(","):
                        try:
                            nxt = driver.find_element(By.CSS_SELECTOR, sel.strip())
                            if nxt.is_enabled():
                                break
                        except Exception:
                            continue
                    if nxt and nxt.is_enabled():
                        driver.execute_script("arguments[0].click();", nxt)
                        time.sleep(1.2)
                        continue
                except Exception:
                    pass
            break  # si pas de pagination, sortir

    finally:
        driver.quit()

    return events


# ---------------------------------
# 3) Orchestration multi‑sites
# ---------------------------------

@dataclass
class AgendaSource:
    school: str
    url: str

# Si vous avez des URLs différentes (ex: page iCal directe), ajoutez‑les ici :
ICS_SOURCES: List[AgendaSource] = [
    # Exemple si une école expose un .ics public :
    # AgendaSource(school="ESSEC Alumni", url="https://www.essecalumni.com/fr/agenda.ics"),
]


def fetch_all_events() -> List[Event]:
    all_events: List[Event] = []

    # 1) iCal directs (si connus)
    for src in ICS_SOURCES:
        all_events.extend(parse_ics(src.url, school=src.school, source=src.url))

    # 2) iCal découverts automatiquement sur les pages d'agenda
    for cfg in SITE_CONFIGS:
        ics_links = find_ics_links(cfg.agenda_url)
        for ics in ics_links:
            all_events.extend(parse_ics(ics, school=cfg.school, source=cfg.agenda_url))

    # 3) Fallback Selenium pour les sites sans iCal exploitable
    #    (et pour compléter des champs manquants)
    for cfg in SITE_CONFIGS:
        scraped = scrape_with_selenium(cfg)
        # Éviter les doublons simples (title+start)
        seen = {(e.title, e.start) for e in all_events}
        for ev in scraped:
            key = (ev.title, ev.start)
            if key not in seen:
                all_events.append(ev)
                seen.add(key)

    return all_events


# ---------------------------------
# 4) Export & mini site HTML
# ---------------------------------

HTML_TEMPLATE = """
<!doctype html>
<html lang="fr">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>Agenda Alumni – Vue d'ensemble</title>
  <style>
    body { font-family: system-ui, -apple-system, Segoe UI, Roboto, Arial, sans-serif; margin: 20px; }
    .filters { display: flex; gap: 12px; flex-wrap: wrap; align-items: center; margin-bottom: 16px; }
    .event { border: 1px solid #e5e7eb; border-radius: 12px; padding: 12px 14px; margin-bottom: 10px; }
    .date { font-weight: 600; }
    .school { font-size: 12px; padding: 2px 8px; border-radius: 999px; background: #eef2ff; display: inline-block; margin-left: 6px; }
    .title { font-size: 18px; margin: 6px 0; }
    .loc { color: #4b5563; font-size: 14px; }
    .muted { color: #6b7280; font-size: 12px; }
    select, input { padding: 6px 8px; border-radius: 8px; border: 1px solid #d1d5db; }
  </style>
</head>
<body>
  <h1>Agenda Alumni – Vue d'ensemble</h1>
  <div class="filters">
    <label>École:
      <select id="schoolFilter"></select>
    </label>
    <label>Rechercher:
      <input id="q" type="search" placeholder="Titre, lieu..." />
    </label>
  </div>
  <div id="list"></div>

  <script>
    const DATA = __DATA__;

    const schoolFilter = document.getElementById('schoolFilter');
    const q = document.getElementById('q');
    const list = document.getElementById('list');

    const schools = Array.from(new Set(DATA.map(d => d.school))).sort();
    schoolFilter.innerHTML = '<option value="">Toutes</option>' + schools.map(s => `<option value="${s}">${s}</option>`).join('');

    function render() {
      const school = schoolFilter.value.trim().toLowerCase();
      const query = q.value.trim().toLowerCase();
      const filtered = DATA.filter(d => {
        const okSchool = !school || d.school.toLowerCase() === school;
        const hay = (d.title||'') + ' ' + (d.location||'');
        const okQuery = !query || hay.toLowerCase().includes(query);
        return okSchool && okQuery;
      }).sort((a,b) => (a.start||'').localeCompare(b.start||''));

      list.innerHTML = filtered.map(d => `
        <div class="event">
          <div class="date">${(d.start||'').replace('T',' ').slice(0,16)} <span class="school">${d.school}</span></div>
          <div class="title">${d.url ? `<a href="${d.url}" target="_blank" rel="noopener">${d.title||'(Sans titre)'}</a>` : (d.title||'(Sans titre)')}</div>
          ${d.location ? `<div class="loc">${d.location}</div>` : ''}
          ${d.raw_date_text ? `<div class="muted">(source date : ${d.raw_date_text})</div>` : ''}
        </div>
      `).join('');
    }

    schoolFilter.addEventListener('change', render);
    q.addEventListener('input', render);
    render();
  </script>
</body>
</html>
"""


def export_outputs(events: List[Event], outdir: str = "output"):
    os.makedirs(outdir, exist_ok=True)
    rows = [asdict(e) for e in events]
    df = pd.DataFrame(rows)

    # Normalisation des dates (évite les NaT non sérialisables)
    if "start" in df.columns:
        df["start"] = pd.to_datetime(df["start"], errors="coerce")
        df = df.sort_values("start", na_position="last")

    csv_path = os.path.join(outdir, "events.csv")
    json_path = os.path.join(outdir, "events.json")
    html_path = os.path.join(outdir, "index.html")

    # Export CSV
    df.to_csv(csv_path, index=False)

    # ✅ JSON-safe : dates ISO + NaT -> null
    data_json = json.loads(df.to_json(orient="records", date_format="iso"))
    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(data_json, f, ensure_ascii=False)

    # Génération HTML statique
    html_out = HTML_TEMPLATE.replace("__DATA__", json.dumps(data_json, ensure_ascii=False))
    with open(html_path, "w", encoding="utf-8") as f:
        f.write(html_out)

    # ✅ Impression finale (pas de retour à la ligne non échappé)
    print(f"✅ Exportés :\n- {csv_path}\n- {json_path}\n- {html_path}")



# ---------------------------------
# 5) Point d'entrée
# ---------------------------------

if __name__ == "__main__":
    print("🔎 Récupération des événements (ICS puis Selenium)…")
    events = fetch_all_events()
    print(f"📦 Événements collectés : {len(events)}")
    export_outputs(events)


# -------------------------------
# 6) Mini viewer Streamlit (opt.)
# -------------------------------
# Sauvegardez ceci dans un fichier séparé `viewer.py` si vous voulez le viewer minimal.
VIEWER_PY = r"""
import pandas as pd
import streamlit as st

st.set_page_config(page_title="Agenda Alumni", layout="wide")

st.title("Agenda Alumni – Vue d'ensemble")

df = pd.read_json("output/events.json")
if "start" in df.columns:
    df["start"] = pd.to_datetime(df["start"], errors="coerce")
    df = df.sort_values("start")

schools = [""] + sorted([s for s in df["school"].dropna().unique()])
sel_school = st.selectbox("École", schools)
q = st.text_input("Rechercher (titre, lieu)…", "")

mask = pd.Series([True]*len(df))
if sel_school:
    mask &= (df["school"] == sel_school)
if q:
    hay = (df["title"].fillna("") + " " + df["location"].fillna("")).str.lower()
    mask &= hay.str.contains(q.lower())

st.dataframe(df.loc[mask, ["start","school","title","location","url"]], use_container_width=True)
"""
