#!/usr/bin/env python3
"""
Enrich artist country via MusicBrainz.

- Číta a upravuje data/playlist.json (pridá/aktualizuje pole "artist_country")
- Udržiava keš v data/artist_cache.json (kľúč = normalizované meno interpreta)
- Rešpektuje rate-limit MB (>= 1 req/s) a používa vlastný User-Agent s emailom
- Používa sentinel CACHE_MISS, aby sme vedeli rozlíšiť "skúšané, nenašlo" od "ešte neskúšané"
- --force zruší CACHE_MISS a lookupne interpretov znova

Kompatibilné s Python 3.11+
"""

from __future__ import annotations

import argparse
import difflib
import json
import os
import re
import time
import unicodedata
from typing import Any, Dict, List, Optional

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import socket
import urllib3.util.connection as urllib3_conn

# ===== Konštanty a nastavenia =====

DEFAULT_INPUT = os.path.join("data", "playlist.json")
DEFAULT_CACHE = os.path.join("data", "artist_cache.json")

MB_URL = "https://musicbrainz.org/ws/2/artist"
NOT_FOUND = "Not found"
CACHE_MISS = "__MISS__"  # interpret sa nenašiel (alebo dočasné zlyhanie pri predchádzajúcom behu)

# (connect timeout, read timeout)
TIMEOUT = (15, 45)

# vynúť IPv4 (niektorým runnerom hapruje IPv6)
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


# ===== Pomocné funkcie (normalizácia, IO) =====

def _ascii_fold(s: str) -> str:
    return unicodedata.normalize("NFKD", s).encode("ascii", "ignore").decode("ascii")

def _clean_name(s: str) -> str:
    # odstráň zátvorky a "feat./ft./featuring ..."
    s = re.sub(r"\s+\(.*?\)", "", s, flags=re.IGNORECASE)
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

def _cache_key_artist(artist: str) -> str:
    return _norm_for_match(artist)


# ===== MusicBrainz lookup =====

def mb_lookup_country(artist: str, email: str) -> Optional[str]:
    """
    Nájde ISO kód krajiny interpreta v MusicBrainz.
    Vráti napr. "DE"/"GB"/"SK" alebo None, ak sa nenašlo.
    """
    if not artist:
        return None

    headers = {
        # podľa MB guideline maj byť UA + kontakt
        "User-Agent": f"radio-playlist-scraper/1.0 ({email})"
    }
    params = {
        "query": f'artist:"{artist}"',
        "fmt": "json",
        "limit": 5,
    }

    try:
        r = _session.get(MB_URL, params=params, headers=headers, timeout=TIMEOUT)
        r.raise_for_status()
        data = r.json()
    except Exception:
        return None

    want = _norm_for_match(artist)
    best = None
    best_score = 0.0

    for a in data.get("artists", []):
        name = a.get("name", "") or ""
        # MB poskytuje "score" 0-100, ale nespoliehame sa len naň
        ratio = difflib.SequenceMatcher(a=_norm_for_match(name), b=want).ratio()
        mb_score = (a.get("score", 0) or 0) / 100.0
        bonus_has_country = 0.05 if (a.get("country") or a.get("area")) else 0.0
        score = 0.7 * ratio + 0.25 * mb_score + bonus_has_country
        if score > best_score:
            best = a
            best_score = score

    if not best:
        return None

    # prioritne priamo "country"
    country = best.get("country")
    if country:
        return country

    # fallback: area → iso-3166-1-codes
    area = best.get("area") or {}
    codes = area.get("iso-3166-1-codes") or []
    if codes:
        return codes[0]

    return None


# ===== Hlavná logika obohatenia =====

def enrich_artist_country(items: List[Dict[str, Any]],
                          cache: Dict[str, Any],
                          email: str,
                          limit: int,
                          pause_s: float,
                          force: bool) -> tuple[int, int]:
    """
    Doplní 'artist_country' do položiek.
    Vráti (počet úspešne doplnených kódov krajín, počet vykonaných lookupov).
    """
    enriched = 0
    lookups = 0

    for it in items:
        # Ak už je vyplnené a nechceme prepísať, preskoč
        if not force and (it.get("artist_country") not in (None, "", NOT_FOUND)):
            continue

        artist = (it.get("artist") or "").strip()
        if not artist:
            it["artist_country"] = NOT_FOUND
            continue

        key = _cache_key_artist(artist)
        state = cache.get(key, None)

        # Ak máme CACHE_MISS a nie je --force, nevyvolávame lookup znovu
        if state == CACHE_MISS and not force:
            it["artist_country"] = NOT_FOUND
            continue

        # Ak máme platnú hodnotu v keši (reťazec krajiny), použijeme ju
        if isinstance(state, str) and state not in (CACHE_MISS, ""):
            it["artist_country"] = state
            continue

        # Tu: nemáme nič v keši alebo ideme znova (--force)
        if lookups >= limit:
            # Nedopĺňaj keš, iba vyznač do položky ako NOT_FOUND pre tento beh
            it["artist_country"] = NOT_FOUND
            continue

        country = mb_lookup_country(artist, email=email)
        lookups += 1
        time.sleep(pause_s)

        if country:
            cache[key] = country
            it["artist_country"] = country
            enriched += 1
        else:
            # Ulož sentinel – nabudúce sa lookup preskočí (pokiaľ nepoužiješ --force)
            cache[key] = CACHE_MISS
            it["artist_country"] = NOT_FOUND

    return enriched, lookups


# ===== CLI =====

def main() -> int:
    ap = argparse.ArgumentParser(description="Enrich artist country via MusicBrainz API.")
    ap.add_argument("--input", default=DEFAULT_INPUT, help="Vstupný JSON s playlistom (default: data/playlist.json)")
    ap.add_argument("--cache", default=DEFAULT_CACHE, help="Cesta ku keši (default: data/artist_cache.json)")
    ap.add_argument("--email", required=True, help="Kontakt do User-Agent (napr. tvoje@meno.sk) – vyžaduje MusicBrainz")
    ap.add_argument("--limit", type=int, default=45, help="Max. lookupov za beh (default: 45)")
    ap.add_argument("--sleep", type=float, default=1.1, help="Pauza medzi lookupmi v sekundách (>=1.0, default: 1.1)")
    ap.add_argument("--force", action="store_true", help="Ignoruj CACHE_MISS a lookupni interpretov znova")
    args = ap.parse_args()

    items = _load_json(args.input, default=None)
    if not isinstance(items, list):
        print(f"Chyba: {args.input} neobsahuje zoznam položiek alebo sa nedá načítať.")
        return 1

    cache = _load_json(args.cache, default={})

    enriched, lookups = enrich_artist_country(
        items=items,
        cache=cache,
        email=args.email,
        limit=args.limit,
        pause_s=args.sleep,
        force=args.force,
    )

    _save_json_atomic(args.input, items)
    _save_json_atomic(args.cache, cache)

    print(f"Artist country enriched: {enriched} (lookups: {lookups})")

    # GitHub Actions summary (ak beží v Actions)
    summary_path = os.environ.get("GITHUB_STEP_SUMMARY")
    if summary_path:
        try:
            with open(summary_path, "a", encoding="utf-8") as s:
                s.write("### Artist Country Enrichment\n")
                s.write(f"- Lookups: **{lookups}**\n")
                s.write(f"- Items enriched: **{enriched}**\n")
        except Exception:
            pass

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
