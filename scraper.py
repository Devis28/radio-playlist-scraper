#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import json
import os
import sys
import time
from datetime import datetime, timedelta, timezone
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup

BASE_URL = "https://www.radia.sk"
PLAYLIST_URL = "https://www.radia.sk/radia/melody/playlist"
OUT_PATH = os.path.join("data", "playlist.json")

HEADERS = {
    "User-Agent": "PlaylistScraper/1.0 (+https://github.com/Devis28/radia-playlist-scraper)"
}
TIMEOUT = 20


def fetch_html(url: str) -> str:
    r = requests.get(url, headers=HEADERS, timeout=TIMEOUT)
    r.raise_for_status()
    return r.text


def normalize_date(raw_date: str) -> str:
    """
    Vstup: '27.09.2025', 'dnes', prípadne 'včera'.
    Výstup: 'DD.MM.RRRR'
    """
    raw = raw_date.strip().lower()
    today = datetime.now().date()
    if raw in ("dnes",):
        d = today
    elif raw in ("včera", "vcera"):
        d = today - timedelta(days=1)
    else:
        # očakávame DD.MM.RRRR
        try:
            d = datetime.strptime(raw, "%d.%m.%Y").date()
        except ValueError:
            # fallback – necháme pôvodný text, ale nech to nespadne
            return raw_date.strip()
    return d.strftime("%d.%m.%Y")


def parse_playlist(html: str):
    """
    Vráti zoznam záznamov: {title, artist, date, time, played_at_iso, track_url}
    """
    soup = BeautifulSoup(html, "html.parser")

    table = soup.select_one("#playlist_table")
    if not table:
        return []

    rows = table.select("div.row.data")
    items = []

    for row in rows:
        # hlavný anchor s dátami o skladbe
        a = row.select_one("a.block.columngroup.datum_cas_skladba")
        if not a:
            # niekedy je štruktúra mierne iná – skúsime fallback
            a = row.find("a")

        if not a:
            continue

        # dátum + čas
        dspan = a.select_one("span.datum")
        tspan = a.select_one("span.cas")
        raw_date = dspan.get_text(strip=True) if dspan else ""
        time_hm = tspan.get_text(strip=True) if tspan else ""

        date_norm = normalize_date(raw_date)

        # interpret + názov
        artist = a.select_one("span.interpret")
        title = a.select_one("span.titul")

        artist_txt = artist.get_text(strip=True) if artist else ""
        title_txt = title.get_text(strip=True) if title else ""

        # link na detail skladby (relatívny -> absolútny)
        href = a.get("href") or ""
        track_url = urljoin(BASE_URL, href)

        # zostav presný timestamp (bez sekúnd – stránka ich neuvádza)
        # použijeme lokálnu časovú zónu a uložíme ISO v UTC pre konzistentnosť
        try:
            local_dt = datetime.strptime(f"{date_norm} {time_hm}", "%d.%m.%Y %H:%M")
            # predpoklad: lokálny čas = Europe/Bratislava (CET/CEST)
            # bez externej knižnice pytz/zoneinfo necháme ako naive -> pripočítame offset podľa systémového času
            # pre jednoduchosť uložíme naive ISO bez Z; zároveň ponecháme date/time polia.
            played_at_iso = local_dt.isoformat()
        except Exception:
            played_at_iso = ""

        items.append(
            {
                "title": title_txt,
                "artist": artist_txt,
                "date": date_norm,          # DD.MM.RRRR
                "time": time_hm,            # HH:MM
                "played_at_iso": played_at_iso,
                "track_url": track_url,
                "source_url": PLAYLIST_URL,
            }
        )
    return items


def load_existing(path: str):
    if not os.path.exists(path):
        return []
    with open(path, "r", encoding="utf-8") as f:
        try:
            return json.load(f)
        except json.JSONDecodeError:
            return []


def unique_key(item: dict) -> str:
    # kľúč pre deduplikáciu – kombinuje presný čas + interpreta + názov
    return f"{item.get('date')} {item.get('time')} | {item.get('artist')} | {item.get('title')}"


def merge_dedup(old: list, new: list) -> list:
    seen = {unique_key(it): it for it in old}
    added = 0
    for it in new:
        k = unique_key(it)
        if k not in seen:
            seen[k] = it
            added += 1
    merged = list(seen.values())
    # utrieď od najnovšej po najstaršiu podľa dátumu+času (ak dostupné), inak ponechaj
    def sort_key(it):
        try:
            return datetime.strptime(f"{it['date']} {it['time']}", "%d.%m.%Y %H:%M")
        except Exception:
            return datetime.min
    merged.sort(key=sort_key, reverse=True)
    print(f"Nové záznamy: {added}", file=sys.stderr)
    return merged


def main():
    os.makedirs(os.path.dirname(OUT_PATH), exist_ok=True)

    html = fetch_html(PLAYLIST_URL)
    new_items = parse_playlist(html)
    if not new_items:
        print("⚠️  Nenašli sa žiadne položky – možno sa zmenilo HTML.", file=sys.stderr)
        return 0

    old_items = load_existing(OUT_PATH)
    merged = merge_dedup(old_items, new_items)

    # ulož JSON v UTF-8, čitateľne
    with open(OUT_PATH, "w", encoding="utf-8") as f:
        json.dump(merged, f, ensure_ascii=False, indent=2)

    print(f"OK – zapísaných {len(merged)} položiek do {OUT_PATH}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
