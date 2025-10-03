import argparse
import json
import os
import sys
import time
import difflib
import unicodedata
import re
import socket
from typing import Optional

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import urllib3.util.connection as urllib3_conn

# ===== Konštanty =====
DEFAULT_INPUT = os.path.join("data", "playlist.json")
DEFAULT_CACHE = os.path.join("data", "enrich_cache.json")
ITUNES_URL = "https://itunes.apple.com/search"

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json,text/plain,*/*",
    "Accept-Language": "sk-SK,sk;q=0.9,en;q=0.8",
    "Referer": "https://itunes.apple.com/",
}

# (connect timeout, read timeout)
TIMEOUT = (15, 45)

# Sentinel hodnoty
NOT_FOUND = "Not found"
CACHE_MISS = "__MISS__"  # skladba sa v iTunes nenašla (pre dané vyhľadávanie)

# vynúť IPv4 – niektorým runnerom hapruje IPv6
def _allowed_gai_family():
    return socket.AF_INET
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

# ===== Pomocné funkcie =====
def _ascii_fold(s: str) -> str:
    return unicodedata.normalize("NFKD", s).encode("ascii", "ignore").decode("ascii")

def _clean_name(s: str) -> str:
    s = re.sub(r"\s+\(.*?\)", "", s, flags=re.IGNORECASE)  # odstráň zátvorky
    s = re.sub(r"\s+(feat\.|ft\.|featuring)\s+.*$", "", s, flags=re.IGNORECASE)
    return " ".join(s.split())

def _norm_for_match(s: str) -> str:
    return _ascii_fold(_clean_name(s)).casefold()

def _load_json(path: str, default):
    if not os.path.exists(path):
        return default
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return default

def _save_json_atomic(path: str, data) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    os.replace(tmp, path)

def _cache_key(artist: str, title: str) -> str:
    return f"{_norm_for_match(artist)}|{_norm_for_match(title)}"

# meta polia, ktoré spravujeme (bez disc_number)
META_KEYS = ("year", "duration_ms", "genre", "album", "track_number")

def _has_all_meta(it: dict) -> bool:
    """Má položka všetky meta polia vyplnené a nie 'Not found'?"""
    for k in META_KEYS:
        v = it.get(k)
        if v in (None, "", NOT_FOUND):
            return False
    return True

def _apply_not_found(item: dict) -> None:
    """Zapíše 'Not found' do všetkých meta polí, ak chýbajú."""
    for k in META_KEYS:
        v = item.get(k)
        if v in (None, "",):
            item[k] = NOT_FOUND

# ===== iTunes lookup =====
def itunes_lookup(artist: str, title: str, country: str) -> Optional[dict]:
    """Vráti meta dict (s 'Not found' pre chýbajúce polia) alebo None pri úplnom zlyhaní/bez zhody."""
    if not artist or not title:
        return None

    params = {
        "term": f"{artist} {title}",
        "country": country,
        "media": "music",
        "entity": "song",
        "limit": 5,
    }
    try:
        resp = _session.get(ITUNES_URL, params=params, headers=HEADERS, timeout=TIMEOUT)
        resp.raise_for_status()
        data = resp.json()
    except Exception:
        return None

    want_a = _norm_for_match(artist)
    want_t = _norm_for_match(title)

    best = None
    best_score = 0.0
    for r in data.get("results", []):
        ra = _norm_for_match(r.get("artistName", "") or "")
        rt = _norm_for_match(r.get("trackName", "") or "")
        s_artist = difflib.SequenceMatcher(a=want_a, b=ra).ratio()
        s_title = difflib.SequenceMatcher(a=want_t, b=rt).ratio()
        score = 0.55 * s_title + 0.45 * s_artist
        if score > best_score:
            best = r
            best_score = score

    if not best or best_score < 0.72:  # prah akceptácie – prípadne doladiť
        return None

    ms = best.get("trackTimeMillis")
    rel_year = (best.get("releaseDate") or "")[:4]
    try:
        year_val = int(rel_year) if rel_year.isdigit() else None
    except Exception:
        year_val = None

    return {
        "year": year_val if year_val is not None else NOT_FOUND,
        "duration_ms": ms if ms is not None else NOT_FOUND,
        "genre": best.get("primaryGenreName") or NOT_FOUND,
        "album": best.get("collectionName") or NOT_FOUND,
        "track_number": best.get("trackNumber") if best.get("trackNumber") is not None else NOT_FOUND,
        "itunes_track_id": best.get("trackId"),
        # voliteľné odkazy do budúcna:
        # "track_url": best.get("trackViewUrl"),
        # "album_url": best.get("collectionViewUrl"),
        # "artist_url": best.get("artistViewUrl"),
        # "preview_url": best.get("previewUrl"),
    }

# ===== Hlavná logika =====
def enrich_items(items: list, cache: dict, country: str, limit: int, pause_s: float, force: bool) -> tuple[int, int]:
    """Doplní meta k položkám. Vráti (pocet_obohatenych, vykonanych_lookupov)."""
    enriched = 0
    lookups = 0

    for it in items:
        # preskoč, ak už má všetko a force nie je zapnutý
        if not force and _has_all_meta(it):
            continue

        artist = (it.get("artist") or "").strip()
        title = (it.get("title") or "").strip()
        if not artist or not title:
            _apply_not_found(it)
            continue

        key = _cache_key(artist, title)
        state = cache.get(key, None)

        # poznáme MISS a nie je --force -> dopíš Not found a pokračuj
        if state == CACHE_MISS and not force:
            before = json.dumps({k: it.get(k) for k in META_KEYS}, ensure_ascii=False)
            _apply_not_found(it)
            after = json.dumps({k: it.get(k) for k in META_KEYS}, ensure_ascii=False)
            if before != after:
                enriched += 1
            continue

        # urob lookup (ak limit dovoľuje) alebo použi keš
        if state is None or force:
            if lookups >= limit:
                if not _has_all_meta(it):
                    before = json.dumps({k: it.get(k) for k in META_KEYS}, ensure_ascii=False)
                    _apply_not_found(it)
                    after = json.dumps({k: it.get(k) for k in META_KEYS}, ensure_ascii=False)
                    if before != after:
                        enriched += 1
                continue

            meta = itunes_lookup(artist, title, country)
            lookups += 1
            time.sleep(pause_s)

            if meta:
                cache[key] = meta
            else:
                cache[key] = CACHE_MISS
                before = json.dumps({k: it.get(k) for k in META_KEYS}, ensure_ascii=False)
                _apply_not_found(it)
                after = json.dumps({k: it.get(k) for k in META_KEYS}, ensure_ascii=False)
                if before != after:
                    enriched += 1
                continue
        else:
            meta = state  # z keše

        if meta and meta != CACHE_MISS:
            before = json.dumps({k: it.get(k) for k in META_KEYS}, ensure_ascii=False)
            it.update({
                "year": meta.get("year", NOT_FOUND),
                "duration_ms": meta.get("duration_ms", NOT_FOUND),
                "genre": meta.get("genre", NOT_FOUND),
                "album": meta.get("album", NOT_FOUND),
                "track_number": meta.get("track_number", NOT_FOUND),
            })
            after = json.dumps({k: it.get(k) for k in META_KEYS}, ensure_ascii=False)
            if before != after:
                enriched += 1

    return enriched, lookups

def main():
    ap = argparse.ArgumentParser(description="Enrich playlist items via iTunes Search API.")
    ap.add_argument("--input", default=DEFAULT_INPUT, help="Vstupný JSON s playlistom")
    ap.add_argument("--cache", default=DEFAULT_CACHE, help="Cesta ku keši (JSON)")
    ap.add_argument("--country", default="sk", help="Kód krajiny pre iTunes (napr. sk/us/cz)")
    ap.add_argument("--limit", type=int, default=40, help="Max. lookupov za beh")
    ap.add_argument("--sleep", type=float, default=0.6, help="Pauza medzi lookupmi (s)")
    ap.add_argument("--force", action="store_true", help="Prepíš aj existujúce meta polia / ignoruj CACHE_MISS")
    args = ap.parse_args()

    # načítaj playlist
    items = _load_json(args.input, default=None)
    if items is None:
        print(f"Chyba: neviem načítať JSON z {args.input}", file=sys.stderr)
        return 1
    if not isinstance(items, list):
        print(f"Chyba: {args.input} neobsahuje zoznam položiek.", file=sys.stderr)
        return 1

    cache = _load_json(args.cache, default={})

    enriched, lookups = enrich_items(items, cache, args.country, args.limit, args.sleep, args.force)

    # zapíš späť
    _save_json_atomic(args.input, items)
    _save_json_atomic(args.cache, cache)

    print(f"iTunes enriched: {enriched} (lookups: {lookups})")
    # GitHub summary (ak beží v Actions)
    summary_path = os.environ.get("GITHUB_STEP_SUMMARY")
    if summary_path:
        with open(summary_path, "a", encoding="utf-8") as s:
            s.write("### Enrichment result\n")
            s.write(f"- iTunes lookups: **{lookups}**\n")
            s.write(f"- Items enriched: **{enriched}**\n")

    return 0

if __name__ == "__main__":
    raise SystemExit(main())
