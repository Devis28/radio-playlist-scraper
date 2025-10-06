#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Enrichuje playlist.json o:
  - artist_country_code (ISO 3166-1 alpha-2) cez MusicBrainz
  - songwriters (zoznam mien) cez MusicBrainz

Zároveň udržiava a publikuje 2 mapovacie JSONy:
  - data/artist_country.json         : { "<artist>": "<ISO>" | null, ... }
  - data/writers_title.json          : { "<artist>|<title>": ["Writer A", ...] | null, ... }

POZNÁMKY:
- writers_title.json kľúčuje podľa "artist|title" (bez normalizácie, len trim).
- prítomná je migrácia zo starej schémy, kde kľúčom bol iba "title":
    - ak pre "<artist>|<title>" kľúč nič nenájdeme a existuje položka s kľúčom "<title>",
      použije sa táto hodnota a zároveň sa doplní aj nový kľúč.
- staré pole `artist_country` sa pri zápise migruje do `artist_country_code` a odstráni.
- texty "Not found" sa konvertujú na None (null).
- Dodržaná pauza >= 1s medzi volaniami MB API (default 1.1s).
"""

import argparse
import difflib
import json
import os
import re
import time
import unicodedata
from typing import Any, List, Optional, Set

import requests

# ===== Cesty a konštanty =====
DEFAULT_INPUT = os.path.join("data", "playlist.json")
ARTIST_COUNTRY_JSON = os.path.join("data", "artist_country.json")   # public map: artist -> country (ISO) or null
WRITERS_TITLE_JSON = os.path.join("data", "writers_title.json")     # public map: artist|title -> [writers] or null

NOT_FOUND = None  # zapisujeme null

# MusicBrainz endpointy
MB_URL_ARTIST = "https://musicbrainz.org/ws/2/artist/"
MB_URL_RECORDING = "https://musicbrainz.org/ws/2/recording"
MB_URL_RECORDING_DETAIL = "https://musicbrainz.org/ws/2/recording/{id}"
MB_URL_WORK = "https://musicbrainz.org/ws/2/work/{id}"

# ===== Utility =====
def _save_atomic(path: str, data: Any):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    os.replace(tmp, path)

def _load_json(path: str, default):
    if not os.path.exists(path):
        return default
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return default

def _clean_name(s: str) -> str:
    s = re.sub(r"\s+", " ", s or "").strip()
    s = re.sub(r"\s+\b(feat|featuring|ft|with|vs)\b\.?.*$", "", s, flags=re.I)
    return s

def _clean_title(s: str) -> str:
    return re.sub(r"\s+", " ", s or "").strip()

def _ascii_fold(s: str) -> str:
    return unicodedata.normalize("NFKD", s).encode("ascii", "ignore").decode("ascii")

def _norm(s: str) -> str:
    return _ascii_fold(_clean_name(s)).casefold()

def _norm_title(s: str) -> str:
    return _ascii_fold(_clean_title(s)).casefold()

SEPARATORS = [" & ", " × ", " x ", " + ", " / ", ";", " and "]

def _reorder_if_surname_first(name: str) -> str:
    # "Patejdl, Vaso" -> "Vaso Patejdl"
    if "," in name:
        left, right = [p.strip() for p in name.split(",", 1)]
        if left and right:
            return f"{right} {left}"
    return name

def _primary_artist(raw: str) -> str:
    s = _clean_name((raw or "").strip())
    for sep in SEPARATORS:
        if sep in s:
            s = s.split(sep, 1)[0].strip()
            break
    s = _reorder_if_surname_first(s)
    return s

def _maybe_swap_order(name: str) -> str:
    # fallback pre "Rolincova Darina" -> "Darina Rolincova"
    parts = (name or "").split()
    if len(parts) == 2:
        a, b = parts
        if len(a) > 2 and len(b) > 2:
            return f"{b} {a}"
    return name

def _writers_key(artist: str, title: str) -> str:
    """Kľúč pre writers mapu — 'artist|title' s trim."""
    return f"{(artist or '').strip()}|{(title or '').strip()}"

# ====== MusicBrainz – ARTIST COUNTRY LOOKUP =====
def mb_lookup_country(artist: str, email: str, timeout=(15, 45)) -> Optional[str]:
    """Vráti ISO 3166-1 alpha-2 kód (napr. 'SK') alebo None."""
    if not artist:
        return None
    headers = {"User-Agent": f"radio-playlist-scraper/1.0 ({email})"}
    params = {"query": f'artist:"{artist}"', "fmt": "json", "limit": 5}
    try:
        r = requests.get(MB_URL_ARTIST, params=params, headers=headers, timeout=timeout)
        r.raise_for_status()
        data = r.json()
    except Exception:
        return None

    want = _norm(artist)
    best, best_score = None, 0.0
    for a in data.get("artists", []):
        name = a.get("name", "")
        ratio = difflib.SequenceMatcher(a=_norm(name), b=want).ratio()
        score = 0.7 * ratio + 0.3 * (a.get("score", 0) / 100.0) + (0.05 if a.get("country") else 0.0)
        if score > best_score:
            best, best_score = a, score

    if not best:
        return None

    country = best.get("country")
    if not country:
        area = best.get("area") or {}
        codes = area.get("iso-3166-1-codes") or []
        country = codes[0] if codes else None
    return country  # môže byť None

# ====== MusicBrainz – SONGWRITER LOOKUP =====
def mb_search_recording(artist: str, title: str, email: str, timeout=(15, 45)) -> Optional[str]:
    """Nájde najvhodnejší recording a vráti jeho ID alebo None."""
    headers = {"User-Agent": f"radio-playlist-scraper/1.0 ({email})"}
    q = f'artist:"{_primary_artist(artist)}" AND recording:"{title}"'
    params = {"query": q, "fmt": "json", "limit": 10}
    try:
        r = requests.get(MB_URL_RECORDING, params=params, headers=headers, timeout=timeout)
        r.raise_for_status()
        data = r.json()
    except Exception:
        return None

    want_title = _norm_title(title)
    want_artist = _norm(_primary_artist(artist))

    best_id, best_score = None, 0.0
    for rec in data.get("recordings", []):
        rec_title = rec.get("title", "")
        rec_score_mb = rec.get("score", 0) / 100.0
        title_ratio = difflib.SequenceMatcher(a=_norm_title(rec_title), b=want_title).ratio()

        # bonus, ak medzi artist-credit je náš primary interpret
        artists = []
        for ac in rec.get("artist-credit", []):
            if isinstance(ac, dict) and "name" in ac:
                artists.append(ac.get("name", ""))
            elif isinstance(ac, str):
                artists.append(ac)
        artist_hit = any(difflib.SequenceMatcher(a=_norm(a), b=want_artist).ratio() > 0.85 for a in artists)

        score = 0.6 * title_ratio + 0.35 * rec_score_mb + (0.05 if artist_hit else 0.0)
        if score > best_score:
            best_score = score
            best_id = rec.get("id")

    return best_id

def mb_get_recording_details(rec_id: str, email: str, timeout=(15, 45)) -> Optional[dict]:
    headers = {"User-Agent": f"radio-playlist-scraper/1.0 ({email})"}
    inc = "artists+releases+work-rels+artist-rels+recording-rels+work-level-rels+writers+composer+lyricist+relations"
    params = {"fmt": "json", "inc": inc}
    try:
        r = requests.get(MB_URL_RECORDING_DETAIL.format(id=rec_id), params=params, headers=headers, timeout=timeout)
        r.raise_for_status()
        return r.json()
    except Exception:
        return None

def mb_get_work(work_id: str, email: str, timeout=(15, 45)) -> Optional[dict]:
    headers = {"User-Agent": f"radio-playlist-scraper/1.0 ({email})"}
    inc = "artist-rels+relations+writers+composer+lyricist"
    params = {"fmt": "json", "inc": inc}
    try:
        r = requests.get(MB_URL_WORK.format(id=work_id), params=params, headers=headers, timeout=timeout)
        r.raise_for_status()
        return r.json()
    except Exception:
        return None

def _collect_writer_names_from_rels(rels: List[dict]) -> Set[str]:
    names = set()
    for rel in rels or []:
        if rel.get("type") in {"writer", "composer", "lyricist", "author"}:
            art = rel.get("artist") or {}
            nm = (art.get("name") or art.get("sort-name") or "").strip()
            if nm:
                names.add(nm)
    return names

def lookup_songwriters(artist: str, title: str, email: str, sleep_s: float, calls_budget: List[int]) -> Optional[List[str]]:
    if not artist or not title:
        return None

    rec_id = mb_search_recording(artist, title, email=email)
    calls_budget[0] -= 1
    time.sleep(sleep_s)

    if not rec_id and calls_budget[0] > 0:
        alt = _maybe_swap_order(_primary_artist(artist))
        if alt and alt != artist:
            rec_id = mb_search_recording(alt, title, email=email)
            calls_budget[0] -= 1
            time.sleep(sleep_s)

    if not rec_id or calls_budget[0] <= 0:
        return None

    det = mb_get_recording_details(rec_id, email=email)
    calls_budget[0] -= 1
    time.sleep(sleep_s)
    if not det:
        return None

    names = set()
    names |= _collect_writer_names_from_rels(det.get("relations") or [])

    # doplň z naviazaných works (limit 2)
    work_rels = [rel for rel in (det.get("relations") or []) if rel.get("target-type") == "work"]
    for rel in work_rels[:2]:
        if calls_budget[0] <= 0:
            break
        wid = (rel.get("work") or {}).get("id")
        if not wid:
            continue
        wdet = mb_get_work(wid, email=email)
        calls_budget[0] -= 1
        time.sleep(sleep_s)
        if wdet:
            names |= _collect_writer_names_from_rels(wdet.get("relations") or [])

    if not names:
        return None
    return sorted(names, key=lambda x: x.casefold())

# ===== Hlavný beh =====
def main():
    ap = argparse.ArgumentParser(
        description="Enrich artist_country_code + songwriters via MusicBrainz and publish mapping JSONs."
    )
    ap.add_argument("--input", default=DEFAULT_INPUT, help="Vstupný JSON s playlistom")
    ap.add_argument("--artist-country-json", default=ARTIST_COUNTRY_JSON, help="Výstupný JSON: artist -> country")
    ap.add_argument("--writers-title-json", default=WRITERS_TITLE_JSON, help="Výstupný JSON: artist|title -> [writers]")
    ap.add_argument("--limit", type=int, default=45, help="Max. lookupov interpretov (country) za beh")
    ap.add_argument("--writers-limit", type=int, default=40, help="Rozpočet API volaní pre songwriter lookup (search+detail+works)")
    ap.add_argument("--sleep", type=float, default=1.1, help="Pauza medzi volaniami (>=1.0s podľa MB guidelines)")
    ap.add_argument("--email", required=True, help="Kontakt do User-Agent, napr. tvoje@meno.sk")
    args = ap.parse_args()

    # načítaj playlist a verejné mapy
    items = _load_json(args.input, default=[])
    if not isinstance(items, list):
        print(f"Chyba: {args.input} neobsahuje zoznam položiek.")
        return 1

    artist_map = _load_json(args.artist_country_json, default={})
    writers_map = _load_json(args.writers_title_json, default={})

    # normalizuj "Not found" na None aj v mapách
    for k, v in list(artist_map.items()):
        if isinstance(v, str) and v.strip().lower() == "not found":
            artist_map[k] = None
    for k, v in list(writers_map.items()):
        if isinstance(v, str) and v.strip().lower() == "not found":
            writers_map[k] = None
        if isinstance(v, list) and len(v) == 0:
            writers_map[k] = None

    enriched_country = 0
    enriched_writers = 0
    lookups_country = 0
    writers_budget = [args.writers_limit]

    for it in items:
        artist_raw = (it.get("artist") or "").strip()
        title_raw  = (it.get("title")  or "").strip()

        # --- migrácia starších polí/hodnôt v playliste ---
        # prenes staré 'artist_country' do 'artist_country_code' (a odstráň ho)
        if "artist_country_code" not in it and "artist_country" in it:
            val = it.get("artist_country")
            if isinstance(val, str) and val.strip().lower() == "not found":
                val = None
            it["artist_country_code"] = val
            it.pop("artist_country", None)

        # normalizuj string "Not found" -> None
        if isinstance(it.get("artist_country_code"), str) and it["artist_country_code"].strip().lower() == "not found":
            it["artist_country_code"] = None
        if isinstance(it.get("songwriters"), str) and it["songwriters"].strip().lower() == "not found":
            it["songwriters"] = None
        if isinstance(it.get("songwriters"), list) and len(it["songwriters"]) == 0:
            it["songwriters"] = None

        # ===== 1) ARTIST COUNTRY CODE =====
        if it.get("artist_country_code") in (None, ""):
            # 1a) z public mapy
            if artist_raw and artist_raw in artist_map:
                it["artist_country_code"] = artist_map[artist_raw]  # môže byť None
            # 1b) MusicBrainz
            elif artist_raw and lookups_country < args.limit:
                country = mb_lookup_country(artist_raw, args.email)
                lookups_country += 1
                time.sleep(args.sleep)
                it["artist_country_code"] = country  # None alebo kód
                artist_map[artist_raw] = country
                if country:
                    enriched_country += 1
            else:
                it["artist_country_code"] = NOT_FOUND
                if artist_raw and artist_raw not in artist_map:
                    artist_map[artist_raw] = NOT_FOUND

        # ===== 2) SONGWRITERS (mapa kľúčovaná cez artist|title) =====
        sw_current = it.get("songwriters")
        if not sw_current:
            key_at = _writers_key(artist_raw, title_raw)

            # 2a) primárne: composite kľúč
            if title_raw and artist_raw and key_at in writers_map:
                it["songwriters"] = writers_map[key_at]  # list alebo None
                if it["songwriters"]:
                    enriched_writers += 1

            # 2b) migrácia zo starej mapy (iba title) – fallback
            elif title_raw and (title_raw in writers_map):
                val = writers_map[title_raw]
                writers_map[key_at] = val
                it["songwriters"] = val
                if it["songwriters"]:
                    enriched_writers += 1

            # 2c) MusicBrainz lookup (ak je rozpočet)
            elif title_raw and artist_raw and writers_budget[0] > 0:
                sw = lookup_songwriters(artist_raw, title_raw, email=args.email,
                                        sleep_s=args.sleep, calls_budget=writers_budget)
                if sw:
                    it["songwriters"] = sw
                    writers_map[key_at] = sw
                    enriched_writers += 1
                else:
                    it["songwriters"] = NOT_FOUND
                    writers_map[key_at] = NOT_FOUND

            else:
                it["songwriters"] = NOT_FOUND
                if title_raw and artist_raw and key_at not in writers_map:
                    writers_map[key_at] = NOT_FOUND

    # zapíš späť
    _save_atomic(args.input, items)
    _save_atomic(args.artist_country_json, artist_map)
    _save_atomic(args.writers_title_json, writers_map)

    print(f"Artist country enriched: {enriched_country} (lookups: {lookups_country})")
    used_sw_calls = args.writers_limit - writers_budget[0]
    print(f"Songwriters enriched: {enriched_writers} (API calls used: {used_sw_calls})")
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
