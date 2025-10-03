import argparse, json, os, time, difflib, unicodedata, re, requests

DEFAULT_INPUT = os.path.join("data", "playlist.json")
DEFAULT_CACHE = os.path.join("data", "artist_cache.json")
NOT_FOUND = "Not found"
MB_URL = "https://musicbrainz.org/ws/2/artist/"

def _ascii_fold(s: str) -> str:
    return unicodedata.normalize("NFKD", s).encode("ascii", "ignore").decode("ascii")

def _clean_name(s: str) -> str:
    s = re.sub(r"\s+\(.*?\)", "", s, flags=re.IGNORECASE)
    s = re.sub(r"\s+(feat\.|ft\.|featuring)\s+.*$", "", s, flags=re.IGNORECASE)
    return " ".join(s.split())

def _norm(s: str) -> str:
    return _ascii_fold(_clean_name(s)).casefold()

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

def _key_artist(artist: str) -> str:
    return _norm(artist)

def mb_lookup_country(artist: str, email: str, timeout=(15,45)) -> str | None:
    if not artist: return None
    headers = {"User-Agent": f"radio-playlist-scraper/1.0 ({email})"}
    params = {"query": f'artist:"{artist}"', "fmt": "json", "limit": 5}
    try:
        r = requests.get(MB_URL, params=params, headers=headers, timeout=timeout)
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
        area = best.get("area", {})
        codes = area.get("iso-3166-1-codes") or []
        country = codes[0] if codes else None
    return country

def main():
    ap = argparse.ArgumentParser(description="Enrich artist country via MusicBrainz.")
    ap.add_argument("--input", default=DEFAULT_INPUT)
    ap.add_argument("--cache", default=DEFAULT_CACHE)
    ap.add_argument("--limit", type=int, default=45)    # dodržuj <= 1 req/s
    ap.add_argument("--sleep", type=float, default=1.1) # MusicBrainz guideline
    ap.add_argument("--email", required=True, help="Kontakt do User-Agent, napr. tvoje@meno.sk")
    args = ap.parse_args()

    items = _load(args.input, default=None)
    if not isinstance(items, list):
        print("Chyba: playlist.json neobsahuje zoznam položiek."); return 1
    cache = _load(args.cache, default={})

    enriched = lookups = 0
    for it in items:
        if "artist_country" in it and it["artist_country"] not in (None, "", NOT_FOUND):
            continue

        artist = (it.get("artist") or "").strip()
        if not artist:
            it["artist_country"] = NOT_FOUND
            continue

        k = _key_artist(artist)
        if k in cache:
            it["artist_country"] = cache[k] or NOT_FOUND
            continue

        if lookups >= args.limit:
            it["artist_country"] = NOT_FOUND
            continue

        country = mb_lookup_country(artist, args.email)
        lookups += 1
        time.sleep(args.sleep)

        cache[k] = country
        it["artist_country"] = country or NOT_FOUND
        if country: enriched += 1

    _save_atomic(args.input, items)
    _save_atomic(args.cache, cache)
    print(f"Artist country enriched: {enriched} (lookups: {lookups})")
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
