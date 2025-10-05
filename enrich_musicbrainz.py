#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import argparse, json, os, time, difflib, unicodedata, re, requests
from typing import Optional, Any, List, Set

# ===== Cesty a konštanty =====
DEFAULT_INPUT = os.path.join("data", "playlist.json")
DEFAULT_ARTIST_CACHE = os.path.join("data", "artist_cache.json")     # krajiny interpretov (ISO kód)
DEFAULT_WRITERS_CACHE = os.path.join("data", "writers_cache.json")   # songwriters podľa artist|title

# Dôležité: používame None => v JSON to bude null (žiadny text "Not found")
NOT_FOUND = None

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

def _key_artist(artist: str) -> str:
    return _norm(artist)

def _key_recording(artist: str, title: str) -> str:
    return f"{_norm(artist)}|{_norm_title(title)}"

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
    ap = argparse.ArgumentParser(description="Enrich artist country code + songwriters via MusicBrainz.")
    ap.add_argument("--input", default=DEFAULT_INPUT, help="Vstupný JSON s playlistom")
    ap.add_argument("--cache", dest="artist_cache", default=DEFAULT_ARTIST_CACHE, help="Keš pre štát interpreta (ISO kód)")
    ap.add_argument("--writers-cache", default=DEFAULT_WRITERS_CACHE, help="Keš pre songwritera (artist|title)")
    ap.add_argument("--limit", type=int, default=45, help="Max. lookupov interpretov (country) za beh")
    ap.add_argument("--writers-limit", type=int, default=40, help="Rozpočet API volaní pre songwriter lookup (search+detail+works)")
    ap.add_argument("--sleep", type=float, default=1.1, help="Pauza medzi volaniami (>=1.0s podľa MB guidelines)")
    ap.add_argument("--email", required=True, help="Kontakt do User-Agent, napr. tvoje@meno.sk")
    args = ap.parse_args()

    # načítaj dáta a cache
    items = json.load(open(args.input, "r", encoding="utf-8")) if os.path.exists(args.input) else []
    artist_cache = json.load(open(args.artist_cache, "r", encoding="utf-8")) if os.path.exists(args.artist_cache) else {}
    writers_cache = json.load(open(args.writers_cache, "r", encoding="utf-8")) if os.path.exists(args.writers_cache) else {}

    # kompatibilita: ak boli v cache staré stringy "Not found", premeň ich na None
    for k, v in list(artist_cache.items()):
        if isinstance(v, str) and v.strip().lower() == "not found":
            artist_cache[k] = None

    enriched_country = 0
    enriched_writers = 0
    lookups_country = 0
    writers_budget = [args.writers_limit]

    for it in items:
        # ===== 1) ARTIST COUNTRY CODE =====
        if it.get("artist_country_code") in (None, ""):
            artist_raw = (it.get("artist") or "").strip()
            if not artist_raw:
                it["artist_country_code"] = NOT_FOUND  # None
            else:
                k_artist = _key_artist(artist_raw)
                cached = artist_cache.get(k_artist)
                # cached môže byť None (čo je v poriadku)
                if cached is not None:
                    it["artist_country_code"] = cached
                elif lookups_country < args.limit:
                    country = mb_lookup_country(artist_raw, args.email)
                    lookups_country += 1
                    time.sleep(args.sleep)
                    artist_cache[k_artist] = country  # môže byť None
                    it["artist_country_code"] = country
                    if country:
                        enriched_country += 1
                else:
                    it["artist_country_code"] = NOT_FOUND  # None

        # ===== 2) SONGWRITERS =====
        if it.get("songwriters") in (None, "", []):
            artist_raw = (it.get("artist") or "").strip()
            title_raw = (it.get("title") or "").strip()
            if not artist_raw or not title_raw:
                it["songwriters"] = NOT_FOUND  # None
            else:
                k_rec = _key_recording(artist_raw, title_raw)
                cached_sw = writers_cache.get(k_rec)
                if isinstance(cached_sw, list) and cached_sw:
                    it["songwriters"] = cached_sw
                elif isinstance(cached_sw, list) and not cached_sw:
                    # v cache je "miss" – nechaj None v dátach
                    it["songwriters"] = NOT_FOUND
                else:
                    if writers_budget[0] <= 0:
                        it["songwriters"] = NOT_FOUND
                    else:
                        sw = lookup_songwriters(artist_raw, title_raw, email=args.email,
                                                sleep_s=args.sleep, calls_budget=writers_budget)
                        if sw:
                            writers_cache[k_rec] = sw
                            it["songwriters"] = sw
                            enriched_writers += 1
                        else:
                            # ulož prázdny list ako "miss", v JSON nechaj None
                            writers_cache[k_rec] = []
                            it["songwriters"] = NOT_FOUND

    _save_atomic(args.input, items)
    _save_atomic(args.artist_cache, artist_cache)
    _save_atomic(args.writers_cache, writers_cache)

    print(f"Artist country code enriched: {enriched_country} (lookups: {lookups_country})")
    print(f"Songwriters enriched: {enriched_writers} (API calls used: {args.writers_limit - writers_budget[0]})")
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
