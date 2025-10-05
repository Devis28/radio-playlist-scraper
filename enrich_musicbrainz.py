#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import argparse, json, os, time, difflib, unicodedata, re, requests
from typing import Optional, Dict, Any, List, Tuple, Set

# ===== Cesty a konštanty =====
DEFAULT_INPUT = os.path.join("data", "playlist.json")
DEFAULT_ARTIST_CACHE = os.path.join("data", "artist_cache.json")   # krajiny interpretov
DEFAULT_WRITERS_CACHE = os.path.join("data", "writers_cache.json") # songwriters podľa artist+title

NOT_FOUND = "Not found"

# MusicBrainz endpointy
MB_URL_ARTIST = "https://musicbrainz.org/ws/2/artist/"
MB_URL_RECORDING = "https://musicbrainz.org/ws/2/recording"
MB_URL_GET_RECORDING = "https://musicbrainz.org/ws/2/recording/{mbid}"
MB_URL_GET_WORK = "https://musicbrainz.org/ws/2/work/{mbid}"

# MB roly, ktoré považujeme za "songwriter" kredit
WRITER_ROLES = {"writer", "composer", "lyricist", "author"}

# ===== Normalizácia textu =====
def _ascii_fold(s: str) -> str:
    return unicodedata.normalize("NFKD", s).encode("ascii", "ignore").decode("ascii")

def _clean_name(s: str) -> str:
    # odstráni zátvorky a 'feat./ft./featuring ...'
    s = re.sub(r"\s+\(.*?\)", "", s, flags=re.IGNORECASE)
    s = re.sub(r"\s+(feat\.|ft\.|featuring)\s+.*$", "", s, flags=re.IGNORECASE)
    return " ".join(s.split())

def _clean_title(s: str) -> str:
    # z titulov väčšinou stačí odstrániť zátvorky typu (Remastered), (Radio Edit) atď.
    s = re.sub(r"\s+\(.*?\)", "", s, flags=re.IGNORECASE)
    return " ".join(s.split())

def _norm(s: str) -> str:
    return _ascii_fold(_clean_name(s)).casefold()

def _norm_title(s: str) -> str:
    return _ascii_fold(_clean_title(s)).casefold()

# rozdeľovače pre multi-interpretov
SEPARATORS = [" & ", " × ", " x ", " + ", " / ", ";", " and "]

def _reorder_if_surname_first(name: str) -> str:
    # "Patejdl, Vaso" -> "Vaso Patejdl"
    if "," in name:
        left, right = [p.strip() for p in name.split(",", 1)]
        if left and right:
            return f"{right} {left}"
    return name

def _primary_artist(raw: str) -> str:
    s = _clean_name(raw.strip())
    for sep in SEPARATORS:
        if sep in s:
            s = s.split(sep, 1)[0].strip()
            break
    s = _reorder_if_surname_first(s)
    return s

def _maybe_swap_order(name: str) -> str:
    # fallback pre "Rolincova Darina" -> "Darina Rolincova"
    parts = name.split()
    if len(parts) == 2:
        return f"{parts[1]} {parts[0]}"
    return name

# ===== IO helpery =====
def _load(path, default):
    if not os.path.exists(path): return default
    try:
        with open(path, "r", encoding="utf-8") as f: return json.load(f)
    except Exception:
        return default

def _save_atomic(path, data):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    os.replace(tmp, path)

# ===== Kľúče do keší =====
def _key_artist(artist: str) -> str:
    # pre krajiny – kľúč len podľa interpreta (normalizovaný prvý interpret)
    return _norm(_primary_artist(artist))

def _key_recording(artist: str, title: str) -> str:
    # pre songwritera – kľúč podľa (prvý interpret, očistený titul)
    return f"{_norm(_primary_artist(artist))}|{_norm_title(title)}"

# ====== MusicBrainz – COUNTRY LOOKUP (artists) =====
def mb_lookup_country(artist: str, email: str, timeout=(15,45)) -> Optional[str]:
    if not artist: return None
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
        name = a.get("name","")
        ratio = difflib.SequenceMatcher(a=_norm(name), b=want).ratio()
        score = 0.7*ratio + 0.3*(a.get("score",0)/100.0) + (0.05 if a.get("country") else 0.0)
        if score > best_score:
            best, best_score = a, score

    if not best: return None
    # country je ISO kód (napr. "SE"); fallback cez area kód
    country = best.get("country")
    if not country:
        area = best.get("area", {}) or {}
        codes = area.get("iso-3166-1-codes") or []
        country = codes[0] if codes else None
    return country

# ====== MusicBrainz – SONGWRITER LOOKUP (recording -> work) =====
def mb_search_recording(artist: str, title: str, email: str, timeout=(15,45)) -> Optional[dict]:
    headers = {"User-Agent": f"radio-playlist-scraper/1.0 ({email})"}
    params = {
        "query": f'recording:"{title}" AND artist:"{artist}"',
        "fmt": "json",
        "limit": 5,
    }
    try:
        r = requests.get(MB_URL_RECORDING, params=params, headers=headers, timeout=timeout)
        r.raise_for_status()
        data = r.json()
    except Exception:
        return None

    want_a = _norm(_primary_artist(artist))
    want_t = _norm_title(title)

    best, best_score = None, 0.0
    for rec in data.get("recordings", []):
        rt = _norm_title(rec.get("title","") or "")
        credit = rec.get("artist-credit") or []
        ra = _norm((credit[0].get("name") if credit else "") or "")
        s_title  = difflib.SequenceMatcher(a=want_t, b=rt).ratio()
        s_artist = difflib.SequenceMatcher(a=want_a, b=ra).ratio()
        mb_score = (rec.get("score", 0) or 0) / 100.0
        score = 0.55*s_title + 0.35*s_artist + 0.10*mb_score
        if score > best_score:
            best, best_score = rec, score

    return best if best and best_score >= 0.72 else None

def mb_get_recording_details(rec_id: str, email: str, timeout=(15,45)) -> Optional[dict]:
    headers = {"User-Agent": f"radio-playlist-scraper/1.0 ({email})"}
    params = {"inc": "artist-credits+artist-rels+work-rels", "fmt": "json"}
    try:
        r = requests.get(MB_URL_GET_RECORDING.format(mbid=rec_id), params=params, headers=headers, timeout=timeout)
        r.raise_for_status()
        return r.json()
    except Exception:
        return None

def mb_get_work_details(work_id: str, email: str, timeout=(15,45)) -> Optional[dict]:
    headers = {"User-Agent": f"radio-playlist-scraper/1.0 ({email})"}
    params = {"inc": "artist-rels", "fmt": "json"}
    try:
        r = requests.get(MB_URL_GET_WORK.format(mbid=work_id), params=params, headers=headers, timeout=timeout)
        r.raise_for_status()
        return r.json()
    except Exception:
        return None

def _collect_writer_names_from_rels(relations: List[dict]) -> Set[str]:
    names: Set[str] = set()
    for rel in relations or []:
        rtype = (rel.get("type") or "").casefold()
        if rtype in WRITER_ROLES:
            artist = (rel.get("artist") or {}).get("name")
            if artist:
                names.add(artist)
    return names

def lookup_songwriters(artist: str, title: str, email: str, sleep_s: float,
                       calls_budget: List[int]) -> Optional[List[str]]:
    """
    Vráti list mien songwriterov alebo None pri zlyhaní/bez zhody.
    calls_budget: jednoprvkové pole s počítadlom zostávajúcich volaní (kvôli --writers-limit)
    """
    if calls_budget[0] <= 0:
        return None

    # 1) recording search
    rec = mb_search_recording(artist, title, email=email)
    calls_budget[0] -= 1
    time.sleep(sleep_s)

    # fallback: skús prehodiť poradie mena (Habera Pavol -> Pavol Habera)
    if not rec and calls_budget[0] > 0:
        alt = _maybe_swap_order(_primary_artist(artist))
        if alt and alt != artist:
            rec = mb_search_recording(alt, title, email=email)
            calls_budget[0] -= 1
            time.sleep(sleep_s)

    if not rec or calls_budget[0] <= 0:
        return None

    # 2) recording details – môžu mať priamo writer/composer/lyricist/author
    det = mb_get_recording_details(rec["id"], email=email)
    calls_budget[0] -= 1
    time.sleep(sleep_s)
    if not det:
        return None

    names = set()
    names |= _collect_writer_names_from_rels(det.get("relations") or [])

    # 3) naviazané works – doplň mená z work relations (limitni sa na 2 worky)
    work_rels = [rel for rel in (det.get("relations") or []) if rel.get("target-type") == "work"]
    for rel in work_rels[:2]:
        if calls_budget[0] <= 0:
            break
        work = rel.get("work") or {}
        wid = work.get("id")
        if not wid:
            continue
        wdet = mb_get_work_details(wid, email=email)
        calls_budget[0] -= 1
        time.sleep(sleep_s)
        if wdet:
            names |= _collect_writer_names_from_rels(wdet.get("relations") or [])

    if not names:
        return None
    return sorted(names, key=lambda x: x.casefold())

# ===== Hlavný beh =====
def main():
    ap = argparse.ArgumentParser(description="Enrich artist country + songwriters via MusicBrainz.")
    ap.add_argument("--input", default=DEFAULT_INPUT, help="Vstupný JSON s playlistom")
    ap.add_argument("--cache", dest="artist_cache", default=DEFAULT_ARTIST_CACHE, help="Keš pre štát interpreta")
    ap.add_argument("--writers-cache", default=DEFAULT_WRITERS_CACHE, help="Keš pre songwritera (artist|title)")
    ap.add_argument("--limit", type=int, default=45, help="Max. lookupov interpretov (country) za beh")
    ap.add_argument("--writers-limit", type=int, default=40, help="Rozpočet API volaní pre songwriter lookup (search+detail+works)")
    ap.add_argument("--sleep", type=float, default=1.1, help="Pauza medzi volaniami (>=1.0s podľa MB guidelines)")
    ap.add_argument("--email", required=True, help="Kontakt do User-Agent, napr. tvoje@meno.sk")
    args = ap.parse_args()

    items = _load(args.input, default=None)
    if not isinstance(items, list):
        print("Chyba: playlist.json neobsahuje zoznam položiek."); return 1

    artist_cache: Dict[str, Any] = _load(args.artist_cache, default={})
    writers_cache: Dict[str, Any] = _load(args.writers_cache, default={})

    enriched_country = 0
    enriched_writers = 0
    lookups_country = 0
    calls_writers_used = 0
    writers_budget = [args.writers_limit]

    for it in items:
        # ===== 1) ARTIST COUNTRY =====
        if it.get("artist_country_code") in (None, "", NOT_FOUND):
            artist_raw = (it.get("artist") or "").strip()
            if not artist_raw:
                it["artist_country_code"] = NOT_FOUND
            else:
                k_artist = _key_artist(artist_raw)
                cached = artist_cache.get(k_artist)
                if cached is not None:
                    it["artist_country_code"] = cached or NOT_FOUND
                elif lookups_country < args.limit:
                    country = mb_lookup_country(artist_raw, args.email)
                    lookups_country += 1
                    time.sleep(args.sleep)
                    artist_cache[k_artist] = country
                    it["artist_country_code"] = country or NOT_FOUND
                    if country: enriched_country += 1
                else:
                    it["artist_country_code"] = NOT_FOUND  # pre tento beh, nabudúce sa doplní

        # ===== 2) SONGWRITERS =====
        if it.get("songwriters") in (None, "", NOT_FOUND):
            artist_raw = (it.get("artist") or "").strip()
            title_raw = (it.get("title") or "").strip()
            if not artist_raw or not title_raw:
                it["songwriters"] = NOT_FOUND
            else:
                k_rec = _key_recording(artist_raw, title_raw)
                cached_sw = writers_cache.get(k_rec)
                if isinstance(cached_sw, list) and cached_sw:
                    it["songwriters"] = cached_sw
                else:
                    # pozor: 1 skladba môže minúť 2–4 volania (search + rec detail + až 2× work)
                    if writers_budget[0] <= 0:
                        it["songwriters"] = NOT_FOUND
                    else:
                        sw = lookup_songwriters(artist_raw, title_raw, email=args.email,
                                                sleep_s=args.sleep, calls_budget=writers_budget)
                        calls_writers_used = (args.writers_limit - writers_budget[0])
                        if sw:
                            writers_cache[k_rec] = sw
                            it["songwriters"] = sw
                            enriched_writers += 1
                        else:
                            # ulož prázdny list ako "miss" – nabudúce sa nebudú míňať volania
                            writers_cache[k_rec] = []
                            it["songwriters"] = NOT_FOUND

    _save_atomic(args.input, items)
    _save_atomic(args.artist_cache, artist_cache)
    _save_atomic(args.writers_cache, writers_cache)

    print(f"Artist country enriched: {enriched_country} (lookups: {lookups_country})")
    print(f"Songwriters enriched: {enriched_writers} (API calls used: {args.writers_limit - writers_budget[0]})")
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
