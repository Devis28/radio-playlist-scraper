import json
import os
import sys
import socket
import random
import time
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

import requests
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import urllib3.util.connection as urllib3_conn

# ===== Konštanty =====
BASE_HOSTS = ["https://www.radia.sk", "https://radia.sk"]
PLAYLIST_PATH = "/radia/melody/playlist?ts=1759395719"
OUT_PATH = os.path.join("data", "playlist.json")
LOCAL_TZ = ZoneInfo("Europe/Bratislava")

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "sk-SK,sk;q=0.9,en;q=0.8",
    "Referer": "https://www.radia.sk/",
}

# (connect timeout, read timeout)
TIMEOUT = (15, 45)

# vynúť IPv4 – niektorým GH runnerom hapruje IPv6 na tomto hoste
def _allowed_gai_family():
    return socket.AF_INET  # IPv4 only
urllib3_conn.allowed_gai_family = _allowed_gai_family

# Session s retry/backoff na timeouty a 5xx/429
_session = requests.Session()
_retries = Retry(
    total=5,
    connect=5,
    read=5,
    status=3,
    backoff_factor=2.0,  # 0s, 2s, 4s, 8s, 16s…
    status_forcelist=[429, 500, 502, 503, 504],
    allowed_methods=["HEAD", "GET", "OPTIONS"],
    raise_on_status=False,
)
_adapter = HTTPAdapter(max_retries=_retries, pool_connections=10, pool_maxsize=10)
_session.mount("https://", _adapter)
_session.mount("http://", _adapter)

# ===== Logika =====

def fetch_html() -> str:
    time.sleep(random.uniform(0.0, 1.5))  # jitter, nech nerazíme server v jednej sekunde
    last_err = None
    for host in BASE_HOSTS:
        try:
            r = _session.get(host + PLAYLIST_PATH, headers=HEADERS, timeout=TIMEOUT)
            r.raise_for_status()
            return r.text
        except requests.RequestException as e:
            last_err = e
    raise last_err


def normalize_date(raw_date: str) -> str:
    """'27.09.2025', 'dnes', 'včera' -> 'DD.MM.RRRR'"""
    raw = raw_date.strip().lower()
    # !!! kľúčová zmena: „dnes“ a „včera“ rátame v Europe/Bratislava
    today = datetime.now(LOCAL_TZ).date()

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
    """Vráti zoznam záznamov: {title, artist, date, time}"""
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

        # vytvor záznam len s požadovanými poliami
        items.append(
            {
                "title": title_txt,
                "artist": artist_txt,
                "date": date_norm,   # DD.MM.RRRR
                "time": time_hm,     # HH:MM
            }
        )
    return items


def load_existing(path: str):
    """Načítaj existujúci JSON a odstráň nežiadané polia (ak by sa historicky vyskytli)."""
    if not os.path.exists(path):
        return []
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except json.JSONDecodeError:
        return []


def unique_key(item: dict) -> str:
    # deduplikácia: dátum + čas + interpret + titul
    return f"{item.get('date')} {item.get('time')} | {item.get('artist')} | {item.get('title')}"


def merge_dedup(old: list, new: list):
    """Zlúči a vráti (merged_list, added_count)."""
    seen = {unique_key(it): it for it in old}
    added = 0
    for it in new:
        k = unique_key(it)
        if k not in seen:
            seen[k] = it
            added += 1
    merged = list(seen.values())

    def sort_key(it):
        try:
            return datetime.strptime(f"{it['date']} {it['time']}", "%d.%m.%Y %H:%M")
        except Exception:
            return datetime.min

    merged.sort(key=sort_key, reverse=True)
    return merged, added

# ===== Hlavný beh =====

def main():
    os.makedirs(os.path.dirname(OUT_PATH), exist_ok=True)

    # Pri sieťovej chybe zaloguj a zlyhaj (exit 1)
    try:
        html = fetch_html()
    except requests.RequestException as e:
        print(f"Network error – skipping this run: {e}", file=sys.stderr)
        summary_path = os.environ.get("GITHUB_STEP_SUMMARY")
        if summary_path:
            with open(summary_path, "a", encoding="utf-8") as s:
                s.write("### Scraper result\n")
                s.write("- Success: **NO** (network error)\n")
                s.write(f"- Error: `{e}`\n")
        return 1  # <- dôležité: červený run

    new_items = parse_playlist(html)
    if not new_items:
        print("Nenašli sa žiadne položky – možno sa zmenilo HTML.", file=sys.stderr)
        return 0

    old_items = load_existing(OUT_PATH)
    merged, added = merge_dedup(old_items, new_items)

    with open(OUT_PATH, "w", encoding="utf-8") as f:
        json.dump(merged, f, ensure_ascii=False, indent=2)

    # logy do stepu + summary
    print(f"Nové záznamy: {added}", file=sys.stderr)
    print(f"OK – zapísaných {len(merged)} položiek do {OUT_PATH}")

    summary_path = os.environ.get("GITHUB_STEP_SUMMARY")
    if summary_path:
        with open(summary_path, "a", encoding="utf-8") as s:
            s.write("### Scraper result\n")
            s.write(f"- New records added: **{added}**\n")
            s.write(f"- Total records: **{len(merged)}**\n")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())