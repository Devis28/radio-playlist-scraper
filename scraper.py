#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import json
import os
import sys
from datetime import datetime, timedelta
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

BASE_URL = "https://www.radia.sk"
PLAYLIST_URL = "https://www.radia.sk/radia/melody/playlist"
OUT_PATH = os.path.join("data", "playlist.json")

# používaj reálnejší User-Agent (niektoré weby blokujú robotov)
HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "sk-SK,sk;q=0.9,en;q=0.8",
}

# (connect timeout, read timeout)
TIMEOUT = (15, 45)

# Session s retry/backoff na timeouty a 5xx/429
_session = requests.Session()
_retries = Retry(
    total=5,
    connect=5,
    read=5,
    status=3,
    backoff_factor=2.0,                    # 0s, 2s, 4s, 8s, 16s…
    status_forcelist=[429, 500, 502, 503, 504],
    allowed_methods=["HEAD", "GET", "OPTIONS"],
    raise_on_status=False,
)
_adapter = HTTPAdapter(max_retries=_retries, pool_connections=10, pool_maxsize=10)
_session.mount("https://", _adapter)
_session.mount("http://", _adapter)


def fetch_html(url: str) -> str:
    r = _session.get(url, headers=HEADERS, timeout=TIMEOUT)
    r.raise_for_status()
    return r.text


def normalize_date(raw_date: str) -> str:
    raw = raw_date.strip().lower()
    today = datetime.now().date()
    if raw in ("dnes",):
        d = today
    elif raw in ("včera", "vcera"):
        d = today - timedelta(days=1)
    else:
        try:
            d = datetime.strptime(raw, "%d.%m.%Y").date()
        except ValueError:
            return raw_date.strip()
    return d.strftime("%d.%m.%Y")


def parse_playlist(html: str):
    soup = BeautifulSoup(html, "html.parser")
    table = soup.select_one("#playlist_table")
    if not table:
        return []

    rows = table.select("div.row.data")
    items = []
    for row in rows:
        a = row.select_one("a.block.columngroup.datum_cas_skladba") or row.find("a")
        if not a:
            continue

        dspan = a.select_one("span.datum")
        tspan = a.select_one("span.cas")
        raw_date = dspan.get_text(strip=True) if dspan else ""
        time_hm = tspan.get_text(strip=True) if tspan else ""
        date_norm = normalize_date(raw_date)

        artist = a.select_one("span.interpret")
        title = a.select_one("span.titul")
        artist_txt = artist.get_text(strip=True) if artist else ""
        title_txt = title.get_text(strip=True) if title else ""

        href = a.get("href") or ""
        track_url = urljoin(BASE_URL, href)

        try:
            local_dt = datetime.strptime(f"{date_norm} {time_hm}", "%d.%m.%Y %H:%M")
            played_at_iso = local_dt.isoformat()
        except Exception:
            played_at_iso = ""

        items.append(
            {
                "title": title_txt,
                "artist": artist_txt,
                "date": date_norm,
                "time": time_hm,
                "played_at_iso": played_at_iso,
                "track_url": track_url,
                "source_url": PLAYLIST_URL,
            }
        )
    return items


def load_existing(path: str):
    if not os.path.exists(path):
        return []
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except json.JSONDecodeError:
        return []


def unique_key(item: dict) -> str:
    return f"{item.get('date')} {item.get('time')} | {item.get('artist')} | {item.get('title')}"


def merge_dedup(old: list, new: list) -> list:
    seen = {unique_key(it): it for it in old}
    for it in new:
        k = unique_key(it)
        if k not in seen:
            seen[k] = it
    merged = list(seen.values())

    def sort_key(it):
        try:
            return datetime.strptime(f"{it['date']} {it['time']}", "%d.%m.%Y %H:%M")
        except Exception:
            return datetime.min

    merged.sort(key=sort_key, reverse=True)
    return merged


def main():
    os.makedirs(os.path.dirname(OUT_PATH), exist_ok=True)

    # <<< KĽÚČOVÁ ZMENA: na sieťovú chybu nepadni, len zaloguj a skonči úspešne >>>
    try:
        html = fetch_html(PLAYLIST_URL)
    except requests.RequestException as e:
        print(f"⚠️  Network error – skipping this run: {e}", file=sys.stderr)
        return 0

    new_items = parse_playlist(html)
    if not new_items:
        print("⚠️  Nenašli sa žiadne položky – možno sa zmenilo HTML.", file=sys.stderr)
        return 0

    old_items = load_existing(OUT_PATH)
    merged = merge_dedup(old_items, new_items)

    with open(OUT_PATH, "w", encoding="utf-8") as f:
        json.dump(merged, f, ensure_ascii=False, indent=2)

    print(f"OK – zapísaných {len(merged)} položiek do {OUT_PATH}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
