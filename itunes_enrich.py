# enrich.py
import json
import os
import re
import sys
import time
from urllib.parse import urlencode

import requests

# Voliteľná cache (ak ju máš v requirements), inak sa len preskočí
try:
    import requests_cache
    requests_cache.install_cache("http_cache", expire_after=60 * 60 * 24 * 30)  # 30 dní
except Exception:
    pass

PLAYLIST_PATH = os.path.join("data", "playlist.json")

# iTunes Search API: nepotrebuje token
ITUNES_COUNTRY = os.environ.get("ITUNES_COUNTRY", "sk")  # prípadne "us" pre širší zásah

# Apple Music Catalog API: potrebuje developer token (JWT)
APPLE_MUSIC_DEV_TOKEN = os.environ.get("APPLE_MUSIC_DEV_TOKEN")  # voliteľné
APPLE_MUSIC_STOREFRONT = os.environ.get("APPLE_MUSIC_STOREFRONT", "sk")

SESSION = requests.Session()
SESSION.headers.update({
    "User-Agent": "radio-playlist-enricher/1.0 (+https://github.com/your/repo)"
})

# ------------ Pomocné -------------
_SPLITERS = [
    r"\s*,\s*", r"\s*;\s*", r"\s*&\s*", r"\s+and\s+", r"\s+AND\s+",
    r"\s+feat\.?\s+", r"\s+ft\.?\s+", r"\s+x\s+"
]

def normalize_text(s: str) -> str:
    s = s or ""
    s = re.sub(r"\s+", " ", s).strip()

    # odsekni featuringy v title (iba pre vyhľadávanie, nie pre ukladanie)
    s = re.sub(r"\s*\(feat\.?.*?\)", "", s, flags=re.IGNORECASE)
    s = re.sub(r"\s*feat\.?.*", "", s, flags=re.IGNORECASE)
    s = re.sub(r"\s*ft\.?.*", "", s, flags=re.IGNORECASE)
    return s

def split_composers(composer_name: str):
    if not composer_name:
        return []
    parts = [composer_name]
    for sp in _SPLITERS:
        new_parts = []
        for p in parts:
            new_parts.extend(re.split(sp, p))
        parts = new_parts
    # očisti mená
    out = []
    for p in parts:
        p = p.strip().strip(",;•·")
        if p and p.lower() not in {"unknown", "various artists"}:
            out.append(p)
    # odstráň duplicitné mená s zachovaním poradia
    seen = set()
    uniq = []
    for n in out:
        k = n.lower()
        if k not in seen:
            uniq.append(n)
            seen.add(k)
    return uniq

def best_match_index(candidates, target_artist, target_title):
    def norm(s):
        return normalize_text(s).lower()

    ta = norm(target_artist)
    tt = norm(target_title)

    # podpora pre "priezvisko meno" -> "meno priezvisko"
    ta_rev = " ".join(reversed(ta.split()))

    def score_pair(a, t):
        a = norm(a); t = norm(t)
        score = 0
        # interpret
        if a == ta or a == ta_rev:
            score += 60
        elif ta in a or a in ta or ta_rev in a:
            score += 40
        # titul
        if t == tt:
            score += 60
        elif tt in t or t in tt:
            score += 40
        return score

    best_i, best_score = -1, -1
    for i, c in enumerate(candidates):
        s = score_pair(c.get("artist",""), c.get("title",""))
        if s > best_score:
            best_i, best_score = i, s
    return best_i, best_score

# ------------ iTunes Search -------------
def itunes_lookup(artist: str, title: str):
    term = f"{artist} {title}"
    params = {
        "term": term,
        "entity": "song",
        "country": ITUNES_COUNTRY,
        "limit": 10,
        "lang": "en_us",
    }
    url = f"https://itunes.apple.com/search?{urlencode(params)}"
    r = SESSION.get(url, timeout=30)
    r.raise_for_status()
    js = r.json()

    results = js.get("results") or []
    if not results:
        return None

    # pripravím kandidátov v jednotnej štruktúre
    cands = []
    for it in results:
        cands.append({
            "artist": it.get("artistName") or "",
            "title": it.get("trackName") or "",
            "raw": it
        })

    bi, score = best_match_index(cands, artist, title)
    if bi < 0 or score < 60:
        return None
    return cands[bi]["raw"]

# ------------ Apple Music Catalog (voliteľne) -------------
def apple_music_search(artist: str, title: str):
    if not APPLE_MUSIC_DEV_TOKEN:
        return None
    term = f"{artist} {title}"
    params = {
        "term": term,
        "types": "songs",
        "limit": 5,
    }
    url = f"https://api.music.apple.com/v1/catalog/{APPLE_MUSIC_STOREFRONT}/search?{urlencode(params)}"
    headers = {"Authorization": f"Bearer {APPLE_MUSIC_DEV_TOKEN}"}
    r = SESSION.get(url, headers=headers, timeout=30)
    if r.status_code == 401:
        # zlý/exp. token
        return None
    r.raise_for_status()
    js = r.json()
    songs = (((js.get("results") or {}).get("songs") or {}).get("data")) or []
    if not songs:
        return None

    cands = []
    for s in songs:
        attr = s.get("attributes") or {}
        cands.append({
            "artist": attr.get("artistName") or "",
            "title": attr.get("name") or "",
            "raw": s
        })
    bi, score = best_match_index(cands, artist, title)
    if bi < 0 or score < 60:
        return None
    return cands[bi]["raw"]

# ------------ Orchestrácia pre jednu skladbu -------------
def enrich_one(artist: str, title: str):
    artist_q = normalize_text(artist)
    title_q = normalize_text(title)

    genre = None
    composers = []

    # 1) iTunes – rýchly a bez tokenu
    it_res = None
    try:
        it_res = itunes_lookup_country(artist_q, title_q)
    except Exception:
        it_res = None

    if it_res:
        genre = it_res.get("primaryGenreName") or genre
        comp = it_res.get("composerName")
        if comp:
            composers = split_composers(comp)

    # 2) Apple Music – lepšie žánre a (niekedy) spoľahlivejší composerName
    am_res = None
    if APPLE_MUSIC_DEV_TOKEN:
        try:
            time.sleep(0.2)  # šetrné volanie
            am_res = apple_music_search(artist_q, title_q)
        except Exception:
            am_res = None

    if am_res:
        attr = am_res.get("attributes") or {}
        # žánre – ak iTunes nič nenašiel, alebo chceš preferovať Apple Music:
        if not genre:
            gnames = attr.get("genreNames") or []
            if gnames:
                genre = gnames[0]
        # songwriter/composer
        comp = attr.get("composerName")
        if comp and not composers:
            composers = split_composers(comp)

    updates = {}
    if genre:
        updates["genre"] = genre
    if composers:
        updates["writers"] = composers

    # (voliteľne si môžeš pridať aj ďalšie polia)
    # if am_res and not updates.get("isrc"):
    #     isrc = (am_res.get("attributes") or {}).get("isrc")
    #     if isrc:
    #         updates["isrc"] = isrc

    return updates


def itunes_lookup_country(artist: str, title: str, country: str):
    term = f"{artist} {title}"
    params = {
        "term": term,
        "entity": "song",
        "country": country,
        "limit": 10,
        "lang": "en_us",
    }
    url = f"https://itunes.apple.com/search?{urlencode(params)}"
    r = SESSION.get(url, timeout=30)
    r.raise_for_status()
    js = r.json()
    results = js.get("results") or []
    if not results:
        return None

    cands = [{"artist": it.get("artistName") or "", "title": it.get("trackName") or "", "raw": it}
             for it in results]
    bi, score = best_match_index(cands, artist, title)
    if bi < 0 or score < 60:
        return None
    return cands[bi]["raw"]


# ------------ Main -------------
def main():
    if not os.path.exists(PLAYLIST_PATH):
        print("playlist.json not found", file=sys.stderr)
        return 1

    with open(PLAYLIST_PATH, "r", encoding="utf-8") as f:
        items = json.load(f)

    changed = 0
    for it in items:
        # neprepíš už vyplnené
        needs_genre = not it.get("genre")
        needs_writers = not it.get("writers")
        if not (needs_genre or needs_writers):
            continue

        artist = it.get("artist", "")
        title = it.get("title", "")
        if not artist or not title:
            continue

        try:
            up = enrich_one(artist, title)
        except Exception as e:
            print(f"enrich failed for {artist} - {title}: {e}", file=sys.stderr)
            continue

        if up:
            # iba dopĺňaj chýbajúce; už existujúce polia nechaj tak
            if needs_genre and up.get("genre"):
                it["genre"] = up["genre"]
            if needs_writers and up.get("writers"):
                it["writers"] = up["writers"]
            changed += 1
            # malá pauza – šetrnosť
            time.sleep(0.15)

    if changed:
        with open(PLAYLIST_PATH, "w", encoding="utf-8") as f:
            json.dump(items, f, ensure_ascii=False, indent=2)
        print(f"Enriched items (iTunes/Apple): {changed}")
    else:
        print("No items enriched; nothing to do.")

    return 0

if __name__ == "__main__":
    raise SystemExit(main())
