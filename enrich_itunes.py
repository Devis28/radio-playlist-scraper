# enrich_itunes.py
# Obohatí položky v data/playlist.json:
# - iTunes (primárne): {year, duration_ms, genre, album, track_number}
# - MusicBrainz (fallback + krajina interpreta): {artist_country_code}
# Ak sa údaj nenájde v oboch zdrojoch, zapíše "Not found".
# V summary ukáže: iTunes lookups, MB lookups, Items enriched (iTunes), Items enriched (MusicBrainz), Items changed.

import argparse
import json
import os
import sys
import time
import difflib
import unicodedata
import re
import socket
from typing import Optional, Tuple
from urllib.parse import urljoin

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import urllib3.util.connection as urllib3_conn

# ===== Konštanty =====
DEFAULT_INPUT = os.path.join("data", "playlist.json")
ITUNES_CACHE = os.path.join("data", "enrich_cache.json")
MB_CACHE = os.path.join("data", "mb_cache.json")

# iTunes
ITUNES_URL = "https://itunes.apple.com/search"
ITUNES_SLEEP_DEFAULT = 0.6   # ~<20 req/min
ITUNES_LIMIT_DEFAULT = 40

# MusicBrainz
MB_WS = "https://musicbrainz.org/ws/2/"
MB_SLEEP_DEFAULT = 1.1       # anonymný limit ~1 req/s
MB_LIMIT_DEFAULT = 30
DEFAULT_MB_UA = "melody-playlist-bot/1.0 (+https://github.com/your/repo; contact: you@example.com)"

HEADERS_ITUNES = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json,text/plain,*/*",
    "Accept-Language": "sk-SK,sk;q=0.9,en;q=0.8",
    "Referer": "https://itunes.apple.com/",
}
HEADERS_MB_BASE = {
    "Accept": "application/json",
}

# (connect timeout, read timeout)
TIMEOUT = (15, 45)

# Sentinel hodnoty
NOT_FOUND = "Not found"
CACHE_MISS = "__MISS__"  # nenájdené v danej službe

# vynúť IPv4 – niektorým runnerom hapruje IPv6
def _allowed_gai_family():
    return socket.AF_INET
urllib3_conn.allowed_gai_family = _allowed_gai_family

# Session s retry/backoff
_session = requests.Session()
_retries = Retry(
    total=5,
    connect=5,
    read=5,
    status=3,
    backoff_factor=2.0,
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

# meta polia (bez duration MM:SS a bez country názvov)
META_KEYS = ("year", "duration_ms", "genre", "album", "track_number")

def _has_all_meta(it: dict) -> bool:
    for k in META_KEYS:
        v = it.get(k)
        if v in (None, "", NOT_FOUND):
            return False
    return True

def _apply_not_found(item: dict) -> None:
    for k in META_KEYS:
        if item.get(k) in (None, ""):
            item[k] = NOT_FOUND

def _update_if_missing(item: dict, meta: dict, keys) -> Tuple[bool, bool]:
    """
    Zapíše meta[key] do item, ale len keď je current hodnota prázdna/Not found.
    Vracia (changed_any, changed_real) – changed_real je True len ak bol zapísaný skutočný údaj (nie "Not found").
    """
    changed = False
    changed_real = False
    for k in keys:
        cur = item.get(k)
        if cur in (None, "", NOT_FOUND):
            val = meta.get(k)
            if val not in (None, ""):
                item[k] = val
                changed = True
                if val != NOT_FOUND:
                    changed_real = True
    return changed, changed_real

# ===== iTunes lookup =====
def itunes_lookup(artist: str, title: str, country: str) -> Optional[dict]:
    """Vráti meta dict (s 'Not found' pre chýbajúce polia) alebo None pri bez-zhody."""
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
        resp = _session.get(ITUNES_URL, params=params, headers=HEADERS_ITUNES, timeout=TIMEOUT)
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

    if not best or best_score < 0.72:
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
    }

# ===== MusicBrainz helpers =====
def _mb_headers(ua: str) -> dict:
    h = dict(HEADERS_MB_BASE)
    h["User-Agent"] = ua or DEFAULT_MB_UA
    return h

def _mb_get(path: str, headers: dict, params: dict) -> Optional[dict]:
    url = urljoin(MB_WS, path)
    try:
        r = _session.get(url, headers=headers, params=params, timeout=TIMEOUT)
        r.raise_for_status()
        data = r.json()
    except Exception:
        return None
    finally:
        time.sleep(_mb_get.MB_SLEEP)  # rate limit
    return data
_mb_get.MB_SLEEP = MB_SLEEP_DEFAULT  # nastaviteľné z CLI

def mb_search_recording(artist: str, title: str, headers: dict) -> Optional[dict]:
    """recording:"title" AND artist:"artist" → najvyššie score."""
    a = artist.replace('"', "'")
    t = title.replace('"', "'")
    q = f'artist:"{a}" AND recording:"{t}"'
    params = {"query": q, "fmt": "json", "limit": 5, "inc": "artist-credits+releases+genres+tags"}
    data = _mb_get("recording", headers, params)
    if not data:
        return None
    recs = data.get("recordings") or []
    if not recs:
        return None
    best = max(recs, key=lambda r: int(r.get("score", 0)))
    if int(best.get("score", 0)) < 70:
        return None
    return best

def _parse_date_tuple(s: Optional[str]) -> Tuple[int, int, int]:
    # "YYYY", "YYYY-MM", "YYYY-MM-DD" → tuple na porovnanie; neznáme = veľké čísla
    if not s:
        return (9999, 12, 31)
    parts = (s.split("-") + ["12", "31"])[:3]
    try:
        y = int(parts[0]) if parts[0].isdigit() else 9999
        m = int(parts[1]) if len(parts) > 1 and parts[1].isdigit() else 12
        d = int(parts[2]) if len(parts) > 2 and parts[2].isdigit() else 31
        return (y, m, d)
    except Exception:
        return (9999, 12, 31)

def _mb_pick_earliest_release(recording: dict) -> Optional[dict]:
    releases = recording.get("releases") or []
    if not releases:
        return None
    releases.sort(key=lambda r: _parse_date_tuple(r.get("date")))
    return releases[0]

def mb_fetch_release_tracks(release_id: str, headers: dict) -> Optional[dict]:
    """release/{id}?inc=recordings+media – pre track_number a presnejšiu dĺžku (ms)."""
    params = {"fmt": "json", "inc": "recordings+media"}
    return _mb_get(f"release/{release_id}", headers, params)

def mb_fetch_artist_area_code(artist_mbid: str, headers: dict) -> str:
    """artist/{id}?inc=area+begin-area → vráti iba ISO kód krajiny (alebo 'Not found')."""
    params = {"fmt": "json", "inc": "area+begin-area"}
    data = _mb_get(f"artist/{artist_mbid}", headers, params)
    if not data:
        return NOT_FOUND
    def _extract_code(area: Optional[dict]) -> Optional[str]:
        if not area:
            return None
        codes = area.get("iso-3166-1-codes") or []
        return codes[0] if codes else None
    code = _extract_code(data.get("area")) or _extract_code(data.get("begin-area"))
    return code or NOT_FOUND

def _mb_genre_from_recording(rec: dict) -> Optional[str]:
    # preferuj "genres" (nový systém), potom najpopulárnejší "tag"
    genres = rec.get("genres") or []
    if isinstance(genres, list) and genres:
        name = genres[0].get("name")
        if name:
            return name
    tags = rec.get("tags") or []
    if isinstance(tags, list) and tags:
        tags_sorted = sorted(tags, key=lambda t: int(t.get("count", 0)), reverse=True)
        if tags_sorted and tags_sorted[0].get("name"):
            return tags_sorted[0]["name"]
    return None

def mb_lookup_fallback(artist: str, title: str, headers: dict) -> Optional[dict]:
    """
    Nájde recording → zober najskoršie vydanie (album názov, rok),
    doplní žáner, dĺžku (ms) a track_number (z release detailu), + krajinu interpreta (ISO kód).
    """
    rec = mb_search_recording(artist, title, headers)
    if not rec:
        return None

    # základné údaje z recording
    length_ms = rec.get("length")  # môže byť None
    genre_mb = _mb_genre_from_recording(rec)

    # najskoršie vydanie
    earliest = _mb_pick_earliest_release(rec)
    album = earliest.get("title") if earliest else None
    year = None
    release_id = None
    if earliest:
        date = earliest.get("date") or ""
        year = date[:4] if date else None
        release_id = earliest.get("id")

    # track_number a presnejšia dĺžka z release detailu
    track_number = None
    if release_id:
        rel = mb_fetch_release_tracks(release_id, headers)
        if rel and rel.get("media"):
            rec_id = rec.get("id")
            try:
                for medium in rel["media"]:
                    for tr in medium.get("tracks", []):
                        r = tr.get("recording") or {}
                        if r.get("id") == rec_id:
                            if tr.get("length") is not None:
                                length_ms = tr.get("length")
                            track_number = tr.get("position")
                            raise StopIteration
            except StopIteration:
                pass

    # krajina interpreta (iba ISO kód)
    artist_credit = rec.get("artist-credit") or []
    artist_mbid = None
    if artist_credit and artist_credit[0].get("artist"):
        artist_mbid = artist_credit[0]["artist"].get("id")
    artist_country_code = NOT_FOUND
    if artist_mbid:
        artist_country_code = mb_fetch_artist_area_code(artist_mbid, headers)

    return {
        "album": album or NOT_FOUND,
        "year": int(year) if year and year.isdigit() else NOT_FOUND,
        "duration_ms": length_ms if length_ms is not None else NOT_FOUND,
        "genre": genre_mb or NOT_FOUND,
        "track_number": track_number if track_number is not None else NOT_FOUND,
        "artist_country_code": artist_country_code or NOT_FOUND,
    }

# ===== Hlavná kombinovaná logika =====
def enrich_items(items: list, it_cache: dict, mb_cache: dict,
                 country_itunes: str, it_limit: int, it_sleep: float,
                 mb_limit: int, mb_sleep: float, mb_ua: str, force: bool) -> tuple[int, int, int, int, int]:
    """
    Obohatí položky: najprv iTunes, potom MB fallback pre chýbajúce polia.
    Vracia (itunes_lookups, mb_lookups, items_enriched_itunes, items_enriched_mb, items_changed).
    Do počtu "items_enriched_*" sa rátajú len položky, kde bol zapísaný aspoň jeden skutočný údaj (nie "Not found").
    """
    _mb_get.MB_SLEEP = mb_sleep
    headers_mb = _mb_headers(mb_ua)

    it_lookups = 0
    mb_lookups = 0
    changed_items = 0

    enriched_itunes_ids = set()
    enriched_mb_ids = set()

    for it in items:
        artist = (it.get("artist") or "").strip()
        title = (it.get("title") or "").strip()
        if not artist or not title:
            _apply_not_found(it)
            it.setdefault("artist_country_code", NOT_FOUND)
            continue

        obj_id = id(it)

        # --- iTunes fáza ---
        need_itunes = force or not _has_all_meta(it)
        if need_itunes:
            key = _cache_key(artist, title)
            state = it_cache.get(key, None)

            if state == CACHE_MISS and not force:
                pass
            elif state is None or force:
                if it_lookups < it_limit:
                    meta = itunes_lookup(artist, title, country_itunes)
                    it_lookups += 1
                    time.sleep(it_sleep)
                    if meta:
                        it_cache[key] = meta
                        changed_any, changed_real = _update_if_missing(it, meta, META_KEYS)
                        if changed_any:
                            changed_items += 1
                        if changed_real:
                            enriched_itunes_ids.add(obj_id)
                    else:
                        it_cache[key] = CACHE_MISS
                # nad limitom – nechaj na MB
            else:
                meta = state  # z keše
                if meta != CACHE_MISS:
                    changed_any, changed_real = _update_if_missing(it, meta, META_KEYS)
                    if changed_any:
                        changed_items += 1
                    if changed_real:
                        enriched_itunes_ids.add(obj_id)

        # --- MB fallback + artist_country_code ---
        still_missing = any(it.get(k) in (None, "", NOT_FOUND) for k in META_KEYS)
        need_country_code = it.get("artist_country_code") in (None, "", NOT_FOUND)

        if still_missing or need_country_code or force:
            mb_key = _cache_key(artist, title)
            state = mb_cache.get(mb_key, None)

            if state == CACHE_MISS and not force:
                it.setdefault("artist_country_code", NOT_FOUND)
            elif state is None or force:
                if mb_lookups < mb_limit:
                    meta_mb = mb_lookup_fallback(artist, title, headers_mb)
                    mb_lookups += 1
                    if meta_mb:
                        mb_cache[mb_key] = meta_mb
                        changed_any_a, changed_real_a = _update_if_missing(it, meta_mb, META_KEYS)
                        changed_any_b, changed_real_b = _update_if_missing(
                            it, meta_mb, ("artist_country_code",)
                        )
                        if changed_any_a or changed_any_b:
                            changed_items += 1
                        if changed_real_a or changed_real_b:
                            enriched_mb_ids.add(obj_id)
                    else:
                        mb_cache[mb_key] = CACHE_MISS
                        it.setdefault("artist_country_code", NOT_FOUND)
                else:
                    it.setdefault("artist_country_code", NOT_FOUND)
            else:
                meta_mb = state
                if meta_mb != CACHE_MISS:
                    changed_any_a, changed_real_a = _update_if_missing(it, meta_mb, META_KEYS)
                    changed_any_b, changed_real_b = _update_if_missing(it, meta_mb, ("artist_country_code",))
                    if changed_any_a or changed_any_b:
                        changed_items += 1
                    if changed_real_a or changed_real_b:
                        enriched_mb_ids.add(obj_id)

        # Zabezpeč, aby chýbajúce meta polia mali aspoň "Not found"
        if not _has_all_meta(it):
            before = json.dumps({k: it.get(k) for k in META_KEYS}, ensure_ascii=False)
            _apply_not_found(it)
            after = json.dumps({k: it.get(k) for k in META_KEYS}, ensure_ascii=False)
            if before != after:
                changed_items += 1

        # Artist country code – ak stále chýba, vyplň "Not found"
        it.setdefault("artist_country_code", NOT_FOUND)

    return it_lookups, mb_lookups, len(enriched_itunes_ids), len(enriched_mb_ids), changed_items

def main():
    ap = argparse.ArgumentParser(description="Enrich playlist via iTunes (primary) with MusicBrainz fallback.")
    ap.add_argument("--input", default=DEFAULT_INPUT, help="Vstupný JSON s playlistom")
    ap.add_argument("--itunes-cache", default=ITUNES_CACHE, help="Cesta ku keši (iTunes)")
    ap.add_argument("--mb-cache", default=MB_CACHE, help="Cesta ku keši (MusicBrainz)")
    ap.add_argument("--country", default="sk", help="Kód krajiny pre iTunes (napr. sk/us/cz)")
    ap.add_argument("--limit", type=int, default=ITUNES_LIMIT_DEFAULT, help="Max. iTunes lookupov za beh")
    ap.add_argument("--sleep", type=float, default=ITUNES_SLEEP_DEFAULT, help="Pauza medzi iTunes lookupmi (s)")
    ap.add_argument("--mb-limit", type=int, default=MB_LIMIT_DEFAULT, help="Max. MusicBrainz lookupov za beh")
    ap.add_argument("--mb-sleep", type=float, default=MB_SLEEP_DEFAULT, help="Pauza medzi MB requestami (s)")
    ap.add_argument("--mb-ua", default=os.environ.get("MB_UA", DEFAULT_MB_UA), help="User-Agent pre MusicBrainz")
    ap.add_argument("--force", action="store_true", help="Ignoruj cache miss a prepíš aj existujúce meta hodnoty")
    args = ap.parse_args()

    # načítaj playlist
    items = _load_json(args.input, default=None)
    if items is None:
        print(f"Chyba: neviem načítať JSON z {args.input}", file=sys.stderr)
        return 1
    if not isinstance(items, list):
        print(f"Chyba: {args.input} neobsahuje zoznam položiek.", file=sys.stderr)
        return 1

    it_cache = _load_json(args.itunes_cache, default={})
    mb_cache = _load_json(args.mb_cache, default={})

    it_lookups, mb_lookups, items_it, items_mb, changed = enrich_items(
        items, it_cache, mb_cache,
        country_itunes=args.country,
        it_limit=args.limit, it_sleep=args.sleep,
        mb_limit=args.mb_limit, mb_sleep=args.mb_sleep, mb_ua=args.mb_ua,
        force=args.force
    )

    # zapíš späť
    _save_json_atomic(args.input, items)
    _save_json_atomic(args.itunes_cache, it_cache)
    _save_json_atomic(args.mb_cache, mb_cache)

    print(
        f"iTunes lookups: {it_lookups} | MusicBrainz lookups: {mb_lookups} | "
        f"Items enriched (iTunes): {items_it} | Items enriched (MusicBrainz): {items_mb} | "
        f"Items changed: {changed}"
    )

    # GitHub summary (ak beží v Actions)
    summary_path = os.environ.get("GITHUB_STEP_SUMMARY")
    if summary_path:
        with open(summary_path, "a", encoding="utf-8") as s:
            s.write("### Enrichment result\n")
            s.write(f"- iTunes lookups: **{it_lookups}**\n")
            s.write(f"- MusicBrainz lookups: **{mb_lookups}**\n")
            s.write(f"- Items enriched (iTunes): **{items_it}**\n")
            s.write(f"- Items enriched (MusicBrainz): **{items_mb}**\n")
            s.write(f"- Items changed (total): **{changed}**\n")

    return 0

if __name__ == "__main__":
    raise SystemExit(main())
