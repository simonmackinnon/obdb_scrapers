#!/usr/bin/env python3
# scrape_be_breweries.py
"""
Scrape Belgian brewery data from multiple sources:

  1. https://www.belgium-mapped-out.com/breweries.html
     – static HTML table, 76 entries with WGS84 lat/lng

  2. https://www.beer-coasters.eu/en/list-of-breweries-from-Belgium.html
     – table of 300+ breweries grouped by city

  3. Google Places Text Search (optional discovery source)
     – activated with --discover-with-places; uses Text Search scoped to Belgium
       to find breweries not covered by the scraped sources

Optionally enrich every record via Google Places (--enrich places --google-api-key KEY),
which adds street address, postal code, phone, lat/lng, verifies the brewery is in BE,
and detects permanently-closed breweries via business_status.

Usage examples:
  # Quick smoke test (5 records per source, no enrichment):
  python scrape_be_breweries.py -o out.csv --test

  # Full scrape, no enrichment:
  python scrape_be_breweries.py -o be_breweries.csv

  # Full scrape + Places enrichment (BE-only filter on by default):
  python scrape_be_breweries.py -o be_breweries_enriched.csv \\
      --enrich places --google-api-key AIza...

  # Full scrape + Places discovery + enrichment:
  python scrape_be_breweries.py -o be_breweries_enriched.csv \\
      --enrich places --discover-with-places --google-api-key AIza...

  # Single source in debug mode:
  python scrape_be_breweries.py -o out.csv --only belgium_mapped_out --debug
"""

import argparse
import csv
import re
import sys
import time
import random
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup
from tqdm import tqdm
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


# ─── Schema ───────────────────────────────────────────────────────────────────
CSV_FIELDS = [
    "id", "name", "brewery_type", "address_1", "address_2", "address_3", "city",
    "state_province", "postal_code", "country", "phone", "website_url",
    "longitude", "latitude",
]

# ─── Belgian province normalisation ──────────────────────────────────────────
BE_PROVINCES = {
    # Dutch names
    "antwerpen": "Antwerp",          "antwerp": "Antwerp",
    "oost-vlaanderen": "East Flanders",
    "west-vlaanderen": "West Flanders",
    "vlaams-brabant": "Flemish Brabant",
    "limburg": "Limburg",
    # French names
    "hainaut": "Hainaut",            "henegouwen": "Hainaut",
    "liège": "Liège",                "liege": "Liège",     "luik": "Liège",
    "luxembourg": "Luxembourg",      "luxemburg": "Luxembourg",
    "namur": "Namur",                "namen": "Namur",
    "brabant wallon": "Walloon Brabant",
    # Brussels
    "brussels": "Brussels Capital Region",
    "bruxelles": "Brussels Capital Region",
    "brussel": "Brussels Capital Region",
    "brussels capital region": "Brussels Capital Region",
    # German community
    "liege": "Liège",
}


def norm_province(s):
    return BE_PROVINCES.get((s or "").strip().lower(), (s or "").strip())


# ─── Brewery type classifier ──────────────────────────────────────────────────
# Belgian brewery names appear in French, Dutch, and occasionally German.
# Patterns ordered most-specific → most-generic; first match wins.
_TYPE_PATTERNS = [
    # brewpub – has a restaurant, bar, or café component
    ("brewpub", re.compile(
        r"café[-\s]?brasserie|brasserie[-\s]?café|brasserie[-\s]?restaurant|"
        r"restaurant[-\s]?brasserie|estaminet|taverne|herberg|eetcafé|eetcafe|"
        r"brouwerij[-\s]?café|café[-\s]?brouwerij|grand\s+café|"
        r"\bcafé\b|\bcafe\b|\bbistro\b|\bpub\b|\btavern\b|"
        r"\brestaurant\b|\bhotel\b|\bbierhal\b|\bbiercafé\b|\bbiercafe\b",
        re.IGNORECASE | re.UNICODE,
    )),
    # nano
    ("nano", re.compile(
        r"\bnano[-\s]?brasserie\b|\bnano[-\s]?brouwerij\b|\bnano[-\s]?brew\b",
        re.IGNORECASE | re.UNICODE,
    )),
    # large – known Belgian industrial brewers
    ("large", re.compile(
        r"\bab[-\s]?inbev\b|\banheuser\b|\binterbrew\b|\bjupiler\b|"
        r"\bstella\s+artois\b|\balken[-\s]?maes\b|\bkronenbourg\b|"
        r"\bpiedboeuf\b|\bhaacht\b|\bpalm\s+breweries\b|"
        r"\bduvel\s+moortgat\b|\bmoortgat\b|"
        r"\bbrasseries\s+de\s+charleroi\b|\bunion\s+brewery\b|\bartois\b",
        re.IGNORECASE | re.UNICODE,
    )),
    # contract
    ("contract", re.compile(
        r"brasserie\s+à\s+façon|brasserie\s+a\s+facon|contract[-\s]?brew|"
        r"contractbrouwerij|huurbrouwerij|loonbrouwerij",
        re.IGNORECASE | re.UNICODE,
    )),
]

# Any of these confirms the record is a brewing operation (for micro default)
_ANY_BREWERY_RE = re.compile(
    r"brouwerij|brasserie|brewery|brewing|brouwen|brasser|"
    r"\bbier\b|\bbière\b|\bbeer\b|craft|artisan|abdij|abbaye|trappist",
    re.IGNORECASE | re.UNICODE,
)


def classify_brewery_type(name, default_micro=False):
    """
    Infer OBDB brewery_type from a Belgian brewery name.
    Trappist and abbey breweries map to 'micro' (OBDB has no abbey type).
    default_micro=True: return 'micro' for any unrecognised name that is
    known to be a confirmed brewery (e.g. from a curated source).
    """
    for btype, pattern in _TYPE_PATTERNS:
        if pattern.search(name or ""):
            return btype

    if _ANY_BREWERY_RE.search(name or ""):
        return "micro"

    if default_micro:
        return "micro"

    return ""


def _places_to_obdb_type(business_status, places_types):
    """Map Google Places business_status + types → OBDB brewery_type string."""
    if business_status == "CLOSED_PERMANENTLY":
        return "closed"

    types_set = set(places_types or [])
    has_bar        = "bar" in types_set
    has_restaurant = bool(types_set & {"restaurant", "meal_delivery", "meal_takeaway", "food", "cafe"})

    if has_bar and has_restaurant:
        return "brewpub"
    if has_bar:
        return "bar"
    if has_restaurant:
        return "brewpub"

    return None


# ─── HTTP session ─────────────────────────────────────────────────────────────
SESSION = requests.Session()
SESSION.headers.update({"User-Agent": "Mozilla/5.0 (BreweryScraper/1.2)"})

_retry = Retry(
    total=6, connect=3, read=3, backoff_factor=0.7,
    status_forcelist=(429, 500, 502, 503, 504),
    allowed_methods=("GET",), respect_retry_after_header=True, raise_on_status=False,
)
_adapter = HTTPAdapter(max_retries=_retry, pool_maxsize=10)
SESSION.mount("https://", _adapter)
SESSION.mount("http://",  _adapter)


def safe_get(url, timeout=30):
    try:
        r = SESSION.get(url, timeout=timeout)
        r.raise_for_status()
        return r
    except Exception as e:
        tqdm.write(f"[WARN] Failed GET {url}: {e}")
        return None


def clean_text(x):
    return re.sub(r"\s+", " ", x).strip() if x else ""


def to_record(**kw):
    rec = {k: "" for k in CSV_FIELDS}
    rec["country"] = "Belgium"
    rec.update(kw)
    rec["id"] = ""
    return rec


def _get_json_with_backoff(url, params, base_pause, debug=False):
    time.sleep(base_pause + random.uniform(0, 0.6))
    try:
        resp = SESSION.get(url, params=params, timeout=30)
        resp.raise_for_status()
        return resp.json()
    except requests.exceptions.RequestException as e:
        if debug:
            print(f"[HTTP] {url} failed: {e}", file=sys.stderr)
        return {"status": "HTTP_ERROR", "error_message": str(e)}


# ─── Source 1: belgium-mapped-out.com ────────────────────────────────────────

def scrape_belgium_mapped_out(limit=None, debug=False):
    """
    Parse the single HTML table from belgium-mapped-out.com/breweries.html.
    Each row: Number | "Brewery, City" | UTM Easting | Northing | Zone | Lat | Lon
    The 'Brewery, City' cell has double-encoded whitespace (Â\\xa0) that is cleaned.
    Lat/Lon are already in WGS84 — no conversion needed.
    """
    url = "https://www.belgium-mapped-out.com/breweries.html"
    r = safe_get(url)
    if not r:
        tqdm.write("[WARN] Could not fetch belgium-mapped-out.com — skipping source.")
        return []

    soup = BeautifulSoup(r.text, "html.parser")
    table = soup.find("table")
    if not table:
        tqdm.write("[WARN] No table found on belgium-mapped-out.com")
        return []

    def clean_cell(text):
        # Remove double-encoded non-breaking space artifacts (Â\xa0 = UTF-8 NBSP read as Latin-1)
        return clean_text(text.replace("Â\xa0", " ").replace("\xa0", " ").replace("Â", ""))

    records = []
    for row in table.find_all("tr"):
        cells = row.find_all(["td", "th"])
        if len(cells) < 7:
            continue

        name_city = clean_cell(cells[1].get_text(" "))
        lat_text  = clean_cell(cells[5].get_text(" "))
        lon_text  = clean_cell(cells[6].get_text(" "))

        # Skip header rows
        try:
            float(lat_text)
            float(lon_text)
        except ValueError:
            continue

        # Split "Brewery Name, City" on the last comma
        if "," in name_city:
            parts = name_city.rsplit(",", 1)
            name = clean_text(parts[0])
            city = clean_text(parts[1])
        else:
            name = name_city
            city = ""

        if not name:
            continue

        btype = classify_brewery_type(name, default_micro=True)

        if debug:
            print(f"[BMO] {name!r} in {city!r} lat={lat_text} lon={lon_text} type={btype!r}",
                  file=sys.stderr)

        records.append(to_record(
            name         = name,
            city         = city,
            brewery_type = btype,
            latitude     = lat_text,
            longitude    = lon_text,
        ))

        if limit and len(records) >= limit:
            break

    return records


# ─── Source 2: beer-coasters.eu ──────────────────────────────────────────────

def scrape_beer_coasters(limit=None, debug=False):
    """
    Parse the brewery list from beer-coasters.eu.
    Using ppns=1000 loads all ~300 Belgian breweries on a single page.
    Table structure: City | Brewery (linked) | Coaster count
    Section-header rows (city spanning all 3 cols) and column-header rows are skipped.
    """
    url = "https://www.beer-coasters.eu/en/list-of-breweries-from-Belgium.html?ppns=1000"
    r = safe_get(url)
    if not r:
        tqdm.write("[WARN] Could not fetch beer-coasters.eu — skipping source.")
        return []

    soup = BeautifulSoup(r.text, "html.parser")
    table = soup.find("table")
    if not table:
        tqdm.write("[WARN] No table found on beer-coasters.eu")
        return []

    SKIP_NAMES = {"city", "brewery", "brasserie", "brouwerij", "quantity", "---", ""}

    records = []
    for row in table.find_all("tr"):
        cells = row.find_all(["td", "th"])

        # Section header rows have one cell spanning multiple columns
        if len(cells) == 1:
            continue

        if len(cells) != 3:
            continue

        city_text    = clean_text(cells[0].get_text(" "))
        brewery_text = clean_text(cells[1].get_text(" "))

        if not brewery_text or brewery_text.lower() in SKIP_NAMES:
            continue
        if "sum of coasters" in brewery_text.lower():
            continue

        btype = classify_brewery_type(brewery_text, default_micro=True)

        if debug:
            print(f"[BC] {brewery_text!r} in {city_text!r} type={btype!r}", file=sys.stderr)

        records.append(to_record(
            name         = brewery_text,
            city         = city_text,
            brewery_type = btype,
        ))

        if limit and len(records) >= limit:
            break

    return records


# ─── Source 3: Google Places discovery ───────────────────────────────────────

def discover_with_places(api_key, pause=3.0, debug=False):
    """
    Use Google Places Text Search to discover Belgian breweries not covered by
    the scraped sources.  Runs three queries (English, French, Dutch) and paginates
    each up to 3 pages (max 60 results per query = up to ~180 unique place IDs).
    Returns lightweight records: name, city, lat/lng, website — full address is
    left for the enrichment pass that runs afterwards.
    """
    if not api_key:
        raise ValueError("Google API key required for Places discovery.")

    base_search  = "https://maps.googleapis.com/maps/api/place/textsearch/json"
    base_details = "https://maps.googleapis.com/maps/api/place/details/json"

    queries = [
        "brewery Belgium",
        "brasserie Belgique",
        "brouwerij België",
    ]

    discovered = {}  # place_id → basic result dict

    for query in queries:
        page_token = None
        for _ in range(3):
            params = {"query": query, "key": api_key, "region": "BE"}
            if page_token:
                params["pagetoken"] = page_token
                time.sleep(2.5)  # Google requires a short delay before using next_page_token
            else:
                time.sleep(pause + random.uniform(0, 0.5))

            resp = SESSION.get(base_search, params=params, timeout=30)
            data = resp.json()
            status = data.get("status")

            if status == "REQUEST_DENIED":
                tqdm.write(
                    f"[WARN] Places discovery: REQUEST_DENIED for '{query}'. "
                    "Check API key has Places API enabled."
                )
                break
            if status not in ("OK", "ZERO_RESULTS"):
                if debug:
                    print(f"[DISC] '{query}' status={status}", file=sys.stderr)
                break

            for place in data.get("results", []):
                pid = place.get("place_id")
                if pid and pid not in discovered:
                    discovered[pid] = place

            page_token = data.get("next_page_token")
            if not page_token:
                break

    tqdm.write(f"[DISC] Places discovery: {len(discovered)} unique place IDs found")

    records = []
    for pid in tqdm(discovered, desc="Fetching discovery details", unit="place"):
        time.sleep(pause + random.uniform(0, 0.4))
        det_json = SESSION.get(base_details, params={
            "place_id": pid,
            "fields": "name,address_components,geometry/location,website,business_status",
            "key": api_key,
        }, timeout=30).json()

        result     = det_json.get("result") or {}
        components = result.get("address_components", [])

        # Keep only confirmed Belgium results
        country_code = next(
            (c.get("short_name", "") for c in components if "country" in c.get("types", [])),
            "",
        )
        if country_code.upper() != "BE":
            if debug:
                print(f"[DISC] Skipping non-BE place: {result.get('name')} ({country_code})",
                      file=sys.stderr)
            continue

        parts = {t: c for c in components for t in c.get("types", [])}
        city  = (
            parts.get("locality",                    {}).get("long_name")
            or parts.get("postal_town",              {}).get("long_name")
            or parts.get("administrative_area_level_2", {}).get("long_name")
            or ""
        )
        loc = (result.get("geometry") or {}).get("location") or {}

        records.append(to_record(
            name        = result.get("name", ""),
            city        = city,
            website_url = result.get("website", ""),
            latitude    = str(loc.get("lat", "")),
            longitude   = str(loc.get("lng", "")),
        ))

    tqdm.write(f"[DISC] {len(records)} BE breweries from Places discovery")
    return records


# ─── Google Places enrichment ─────────────────────────────────────────────────

def enrich_with_places(rows, api_key, pause=3.0, test_limit=None, debug=False, strict_be=True):
    """
    For each record look it up via Google Places FindPlace → TextSearch (fallback),
    then fetch full address, phone, website, lat/lng, and business_status from Details.

    strict_be=True (default): drop records confirmed to be outside Belgium.
    Records where the API fails or returns no address_components are kept.

    business_status=CLOSED_PERMANENTLY → brewery_type set to 'closed'.
    """
    if not api_key:
        raise ValueError("Google API key required for Places enrichment.")

    base_find    = "https://maps.googleapis.com/maps/api/place/findplacefromtext/json"
    base_search  = "https://maps.googleapis.com/maps/api/place/textsearch/json"
    base_details = "https://maps.googleapis.com/maps/api/place/details/json"

    def make_query(r):
        bits = [r.get("name", "")]
        if r.get("city"):           bits.append(r["city"])
        if r.get("state_province"): bits.append(r["state_province"])
        bits.append("brewery Belgium")
        return " ".join(b for b in bits if b).strip()

    def log(msg):
        if debug:
            print(msg, file=sys.stderr)

    _EARLY_CHECK  = 5
    _api_errors   = 0
    _api_miss     = 0
    _country_drop = 0

    out      = []
    seen_ids = set()

    for i, r in enumerate(tqdm(rows, desc="Enriching via Google Places", unit="brewery"), 1):
        if test_limit and i > test_limit:
            break

        q = make_query(r)
        if not q:
            out.append(r)
            seen_ids.add(id(r))
            continue

        # ── 1) Find Place ──────────────────────────────────────────────────
        find_json = _get_json_with_backoff(
            base_find,
            {"input": q, "inputtype": "textquery", "fields": "place_id,name", "key": api_key},
            base_pause=pause, debug=debug,
        )
        status   = find_json.get("status")
        place_id = None

        if status == "OK":
            place_id = (find_json.get("candidates") or [{}])[0].get("place_id")
        elif status in ("REQUEST_DENIED", "OVER_QUERY_LIMIT", "INVALID_REQUEST", "HTTP_ERROR"):
            if i <= _EARLY_CHECK:
                _api_errors += 1
                tqdm.write(
                    f"[WARN] Places API returned '{status}' for '{q}'. "
                    "Check your API key has 'Places API' enabled and billing is active."
                )
            if _api_errors >= _EARLY_CHECK:
                tqdm.write(
                    "[ERROR] First 5 Places calls all failed — aborting enrichment.\n"
                    "        Re-run with --enrich none to get unenriched results."
                )
                for remaining in rows[i - 1:]:
                    if id(remaining) not in seen_ids:
                        out.append(remaining)
                        seen_ids.add(id(remaining))
                return out
            out.append(r)
            seen_ids.add(id(r))
            continue

        # ── 1b) Fallback: text search scoped to BE ─────────────────────────
        if not place_id:
            search_json = _get_json_with_backoff(
                base_search,
                {"query": q, "key": api_key, "region": "BE"},
                base_pause=pause, debug=debug,
            )
            if search_json.get("status") == "OK" and search_json.get("results"):
                place_id = search_json["results"][0]["place_id"]
            else:
                log(f"[MISS] No Places match for '{q}'")
                _api_miss += 1
                out.append(r)
                seen_ids.add(id(r))
                continue

        # ── 2) Place Details ───────────────────────────────────────────────
        det_json = _get_json_with_backoff(
            base_details,
            {
                "place_id": place_id,
                "fields": (
                    "name,formatted_address,address_components,"
                    "international_phone_number,website,geometry/location,"
                    "business_status,types"
                ),
                "key": api_key,
            },
            base_pause=pause, debug=debug,
        )
        result     = det_json.get("result") or {}
        components = result.get("address_components", [])
        parts      = {t: c for c in components for t in c.get("types", [])}

        # ── BE country filter ──────────────────────────────────────────────
        country_code = ""
        for c in components:
            if "country" in (c.get("types") or []):
                country_code = c.get("short_name") or ""
                break

        if strict_be and country_code and country_code.upper() != "BE":
            log(f"[FILTER] Excluding non-BE '{result.get('name')}' (country={country_code})")
            _country_drop += 1
            continue

        if not components:
            log(f"[WARN] No address_components for '{q}' — keeping scraped data as-is")
            out.append(r)
            seen_ids.add(id(r))
            continue

        # ── Parse address_components ───────────────────────────────────────
        street_number  = parts.get("street_number",  {}).get("long_name",  "")
        route          = parts.get("route",           {}).get("long_name",  "")
        address_1      = " ".join(x for x in [street_number, route] if x)
        address_2      = parts.get("subpremise",      {}).get("long_name",  "")
        city           = (
            parts.get("locality",                    {}).get("long_name")
            or parts.get("postal_town",              {}).get("long_name")
            or parts.get("administrative_area_level_2", {}).get("long_name")
            or ""
        )
        state_province = norm_province(
            parts.get("administrative_area_level_1", {}).get("long_name", "")
        )
        postal_code    = parts.get("postal_code",     {}).get("long_name",  "")

        # ── Merge (Places fills blank fields; name-based type is a fallback) ─
        if address_1:      r["address_1"]      = address_1
        if address_2:      r["address_2"]      = address_2
        if city:           r["city"]           = city
        if state_province: r["state_province"] = state_province
        if postal_code:    r["postal_code"]    = postal_code
        r["country"]     = "Belgium"
        r["name"]        = r.get("name") or result.get("name", "")
        r["phone"]       = r.get("phone")        or result.get("international_phone_number", "")
        r["website_url"] = r.get("website_url")  or result.get("website", "")

        # ── Brewery type: Places overrides name heuristic ──────────────────
        business_status = result.get("business_status", "")
        places_types    = result.get("types", [])
        places_btype    = _places_to_obdb_type(business_status, places_types)
        if places_btype:
            r["brewery_type"] = places_btype
            log(f"[TYPE] {r['name']!r}: Places→{places_btype!r} "
                f"(status={business_status!r}, types={places_types})")
        elif not r.get("brewery_type"):
            r["brewery_type"] = classify_brewery_type(r.get("name", ""))

        loc = (result.get("geometry") or {}).get("location") or {}
        if not r.get("latitude")  and loc.get("lat") is not None:
            r["latitude"]  = str(loc["lat"])
        if not r.get("longitude") and loc.get("lng") is not None:
            r["longitude"] = str(loc["lng"])

        out.append(r)
        seen_ids.add(id(r))

    if debug or _country_drop or _api_miss:
        tqdm.write(
            f"[INFO] Enrichment: {len(out)} kept, {_country_drop} dropped (non-BE), "
            f"{_api_miss} no-match (kept unchanged)"
        )

    if not strict_be:
        for row in rows:
            if id(row) not in seen_ids:
                out.append(row)

    return out


# ─── Post-processing ──────────────────────────────────────────────────────────

def filter_be_only(rows):
    out = []
    for r in rows:
        if r.get("country") == "Belgium":
            out.append(r)
        elif r.get("address_1", "").lower().endswith("belgium"):
            r["country"] = "Belgium"
            out.append(r)
    return out


def dedupe(records):
    seen, out = set(), []
    for r in tqdm(records, desc="Deduping records", unit="rec"):
        key = (r["name"].lower(), r.get("city", "").lower())
        if key not in seen:
            seen.add(key)
            out.append(r)
    return out


def write_csv(rows, out_path):
    with open(out_path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=CSV_FIELDS)
        w.writeheader()
        for r in tqdm(rows, desc="Writing CSV", unit="row"):
            w.writerow(r)


# ─── Entry point ──────────────────────────────────────────────────────────────

def main():
    ap = argparse.ArgumentParser(
        description="Scrape Belgian breweries from multiple sources and export CSV."
    )
    ap.add_argument("-o", "--output",  required=True, help="Output CSV file path.")
    ap.add_argument("--test",          action="store_true",
                    help="Test mode: scrape ~5 records per source and exit.")
    ap.add_argument("--debug",         action="store_true",
                    help="Verbose debug logs to stderr.")
    ap.add_argument(
        "--only",
        choices=["belgium_mapped_out", "beer_coasters"],
        help="Scrape only one source (useful for debugging)."
    )
    ap.add_argument(
        "--enrich", choices=["places", "none"], default="none",
        help="Enrich with Google Places API (adds address, phone, lat/lng, closed status). Default: none."
    )
    ap.add_argument("--google-api-key",
                    help="Google Places API key (required if --enrich places or --discover-with-places).")
    ap.add_argument("--places-rate", type=float, default=3.0,
                    help="Seconds to pause between Places API calls (default: 3.0).")
    ap.add_argument(
        "--discover-with-places", action="store_true",
        help=(
            "Use Google Places Text Search to find additional Belgian breweries "
            "not covered by the scraped sources. Requires --google-api-key. "
            "Runs three queries (English/French/Dutch) and paginates to ~180 results."
        )
    )
    ap.add_argument(
        "--strict-be",  action="store_true", default=True,
        help="Only keep breweries whose Google Place resolves to Belgium (default: on)."
    )
    ap.add_argument(
        "--no-strict-be", dest="strict_be", action="store_false",
        help="Disable Belgium-only filtering."
    )
    ap.add_argument(
        "--diagnose-places", action="store_true",
        help="Test the Places API with one query and print the raw response, then exit."
    )

    args  = ap.parse_args()
    limit = 5 if args.test else None

    # ── Quick API diagnostic ──────────────────────────────────────────────────
    if args.diagnose_places:
        if not args.google_api_key:
            raise SystemExit("ERROR: --google-api-key is required for --diagnose-places.")
        import json as _json
        key = args.google_api_key
        q   = "Brasserie de la Senne Bruxelles brewery Belgium"
        print(f"Diagnosing Google Places API with query: {q!r}\n")

        base_find    = "https://maps.googleapis.com/maps/api/place/findplacefromtext/json"
        base_search  = "https://maps.googleapis.com/maps/api/place/textsearch/json"
        base_details = "https://maps.googleapis.com/maps/api/place/details/json"

        r1 = SESSION.get(base_find, params={
            "input": q, "inputtype": "textquery", "fields": "place_id,name", "key": key
        }, timeout=30)
        j1 = r1.json()
        print("=== FindPlace ===")
        print(_json.dumps(j1, indent=2, ensure_ascii=False)[:800])

        place_id = (j1.get("candidates") or [{}])[0].get("place_id") if j1.get("status") == "OK" else None
        if not place_id:
            r2 = SESSION.get(base_search, params={"query": q, "key": key, "region": "BE"}, timeout=30)
            j2 = r2.json()
            print("\n=== TextSearch fallback ===")
            print(_json.dumps(j2, indent=2, ensure_ascii=False)[:800])
            place_id = j2["results"][0]["place_id"] if j2.get("status") == "OK" and j2.get("results") else None

        if place_id:
            print(f"\nplace_id: {place_id}")
            r3 = SESSION.get(base_details, params={
                "place_id": place_id,
                "fields": "name,formatted_address,address_components,geometry/location,business_status,types",
                "key": key
            }, timeout=30)
            j3 = r3.json()
            print("\n=== Place Details ===")
            print(_json.dumps(j3, indent=2, ensure_ascii=False)[:1200])
            components = (j3.get("result") or {}).get("address_components", [])
            country = next(
                (c.get("short_name") for c in components if "country" in c.get("types", [])),
                "(not found)"
            )
            print(f"\n→ country_code: {country!r}")
        else:
            print("\nNo place_id found — Places API may not be enabled for this key.")
        raise SystemExit(0)

    # ── Scrape sources ────────────────────────────────────────────────────────
    sources_map = {
        "belgium_mapped_out": (
            "belgium-mapped-out.com",
            lambda lim: scrape_belgium_mapped_out(lim, debug=args.debug),
        ),
        "beer_coasters": (
            "beer-coasters.eu",
            lambda lim: scrape_beer_coasters(lim, debug=args.debug),
        ),
    }

    if args.only:
        active = {args.only: sources_map[args.only]}
    else:
        active = sources_map

    records = []
    for _key, (src_name, func) in tqdm(
        active.items(), total=len(active), desc="Scraping sources", unit="source"
    ):
        recs = []
        try:
            recs = func(limit)
        except Exception as e:
            tqdm.write(f"[WARN] {src_name} scraping error: {e}")
        records += recs
        tqdm.write(f"[INFO] {src_name}: {len(recs)} found  (running total: {len(records)})")

    # ── Optional: Places discovery ────────────────────────────────────────────
    if args.discover_with_places:
        if not args.google_api_key:
            raise SystemExit("ERROR: --google-api-key is required for --discover-with-places.")
        tqdm.write("[INFO] Discovering additional breweries via Google Places…")
        try:
            disc_recs = discover_with_places(
                args.google_api_key, pause=args.places_rate, debug=args.debug
            )
            records += disc_recs
            tqdm.write(f"[INFO] Places discovery: {len(disc_recs)} found  "
                       f"(running total: {len(records)})")
        except Exception as e:
            tqdm.write(f"[WARN] Places discovery error: {e}")

    tqdm.write(f"[INFO] Collected {len(records)} raw records")
    records = dedupe(records)
    tqdm.write(f"[INFO] After dedup: {len(records)} breweries")

    # ── Optional: Places enrichment ───────────────────────────────────────────
    if args.enrich == "places":
        if not args.google_api_key:
            raise SystemExit("ERROR: --google-api-key is required when --enrich places is used.")
        tqdm.write("[INFO] Enriching via Google Places…")
        records = enrich_with_places(
            records,
            args.google_api_key,
            pause      = args.places_rate,
            test_limit = 20 if args.test else None,
            debug      = args.debug,
            strict_be  = args.strict_be,
        )
        tqdm.write(f"[INFO] After Places enrichment: {len(records)} records")
        if args.strict_be:
            records = filter_be_only(records)
            tqdm.write(f"[INFO] After BE-only filter: {len(records)} records")

    write_csv(records, args.output)
    tqdm.write(f"[DONE] Written {len(records)} rows → {args.output}")

    if args.test:
        tqdm.write("[TEST MODE] Partial scrape complete.")


if __name__ == "__main__":
    main()
