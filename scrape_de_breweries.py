#!/usr/bin/env python3
# scrape_de_breweries.py
"""
Scrape German brewery data from two sources:

  1. http://www.german-breweries.com/all_breweries.htm
     – crawl the index, follow each brewery detail link to extract address.

  2. https://en.wikipedia.org/wiki/List_of_brewing_companies_in_Germany
     – parse all wikitables, derive city / state / type from columns.

Optionally enrich every record via Google Places (--enrich places --google-api-key KEY),
which adds street address, postal code, phone, lat/lng and verifies the brewery is in DE.

Usage examples:
  # Quick smoke test (5 records per source, no enrichment):
  python scrape_de_breweries.py -o out.csv --test

  # Full scrape, no enrichment:
  python scrape_de_breweries.py -o de_breweries.csv

  # Full scrape + Places enrichment (Germany-only filter on by default):
  python scrape_de_breweries.py -o de_breweries_enriched.csv \\
      --enrich places --google-api-key AIza...

  # Single source in debug mode:
  python scrape_de_breweries.py -o out.csv --only wikipedia --debug
"""

import argparse
import csv
import re
import sys
import time
import random
from urllib.parse import urljoin, urlparse

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

# ─── Brewery type classifier ─────────────────────────────────────────────────
# German name patterns → OBDB brewery_type.  First match wins.
# Patterns are ordered from most-specific to most-generic.
_TYPE_PATTERNS = [
    # brewpub / Gasthausbrauerei — has a restaurant, hotel, or pub component
    ("brewpub", re.compile(
        r"brauhaus|braugasthof|gasthofbrauerei|gasthausbrauerei|braugasthaus|"
        r"braugasth|braugaststätte|braugaststatte|bräustüberl|braustuberl|"
        r"bräustuberl|schankbrauerei|bierhaus|bierpalast|bierstube|bierkeller|"
        r"gasthof[-\s]?brauerei|gasthaus[-\s]?brauerei|"
        r"brauerei[-\s]?und[-\s]?gasthof|brauerei[-\s]?gaststätte|"
        r"brauerei[-\s]?gaststatte|bierbrauhaus|"
        r"\bcafé\b|\bcafe\b|\bwirtshaus\b|\bgasthof\b|\bgasthaus\b|"
        r"\brestaurant\b|\bhotel\b|\bgaststätte\b|\bgaststatte\b|"
        r"\bkneipe\b|\bschenke\b|\bpinte\b|\bschlachthof\b",
        re.IGNORECASE | re.UNICODE,
    )),
    # nano — explicitly nano-scale
    ("nano", re.compile(
        r"\bnano[-\s]?brauerei\b|\bnanobrau\b|\bnano[-\s]?brew\b",
        re.IGNORECASE | re.UNICODE,
    )),
    # large — industrial-scale or brewery group
    ("large", re.compile(
        r"großbrauerei|grossbrauerei|braugruppe|braukonzern|konzernbrauerei",
        re.IGNORECASE | re.UNICODE,
    )),
    # contract — lohnbrauerei / contract brewing only
    ("contract", re.compile(
        r"vertragsbrauerei|lohnbrauerei|contract[-\s]?brew",
        re.IGNORECASE | re.UNICODE,
    )),
]

# Any of these in the name confirms it's a brewing operation
_ANY_BREWERY_RE = re.compile(
    r"brauerei|bräuerei|bräu\b|brau\b|brewery|brewing|"
    r"brauwerk|bierwerk|biermanufaktur|biermanufactur|"
    r"\bbier\b|\bbräu\b|manufaktur|sudwerk|sudhaus|hopfen|malz",
    re.IGNORECASE | re.UNICODE,
)


def classify_brewery_type(name, has_taproom=False, default_micro=False):
    """
    Infer OBDB brewery_type from a German brewery name + optional taproom flag.

    default_micro=True: return 'micro' for any unrecognised name rather than ''.
    Use this when you know the record is definitely a brewery (e.g. from german-breweries.com).
    """
    for btype, pattern in _TYPE_PATTERNS:
        if pattern.search(name or ""):
            return btype

    if has_taproom:
        return "brewpub"

    if _ANY_BREWERY_RE.search(name or ""):
        return "micro"

    if default_micro:
        return "micro"   # caller guarantees it's a brewery even without keyword evidence

    return ""            # uncertain — leave blank for Places API to fill


def _places_to_obdb_type(business_status, places_types):
    """
    Map Google Places business_status + types list → OBDB brewery_type string.
    Returns None when Places gives no actionable signal (caller keeps existing type).
    """
    if business_status == "CLOSED_PERMANENTLY":
        return "closed"

    types_set = set(places_types or [])
    has_bar        = "bar" in types_set
    has_restaurant = bool(types_set & {"restaurant", "meal_delivery", "meal_takeaway",
                                        "food", "cafe"})

    if has_bar and has_restaurant:
        return "brewpub"
    if has_bar:
        return "bar"
    if has_restaurant:
        return "brewpub"

    return None   # no useful signal


# ─── HTTP session with exponential-backoff retry ──────────────────────────────
SESSION = requests.Session()
SESSION.headers.update({"User-Agent": "Mozilla/5.0 (BreweryScraper/1.2)"})

_retry = Retry(
    total=6,
    connect=3,
    read=3,
    backoff_factor=0.7,
    status_forcelist=(429, 500, 502, 503, 504),
    allowed_methods=("GET",),
    respect_retry_after_header=True,
    raise_on_status=False,
)
_adapter = HTTPAdapter(max_retries=_retry, pool_maxsize=10)
SESSION.mount("https://", _adapter)
SESSION.mount("http://",  _adapter)

# ─── German Bundesländer (name & abbreviation → canonical English name) ───────
DE_STATES = {
    # abbreviations
    "bw": "Baden-Württemberg",  "by": "Bavaria",               "be": "Berlin",
    "bb": "Brandenburg",        "hb": "Bremen",                 "hh": "Hamburg",
    "he": "Hesse",              "mv": "Mecklenburg-Vorpommern", "ni": "Lower Saxony",
    "nw": "North Rhine-Westphalia", "nrw": "North Rhine-Westphalia",
    "rp": "Rhineland-Palatinate",   "sl": "Saarland",
    "sn": "Saxony",             "st": "Saxony-Anhalt",          "sh": "Schleswig-Holstein",
    "th": "Thuringia",
    # German names
    "baden-württemberg": "Baden-Württemberg", "baden-wuerttemberg": "Baden-Württemberg",
    "bavaria": "Bavaria",       "bayern": "Bavaria",
    "berlin": "Berlin",         "brandenburg": "Brandenburg",
    "bremen": "Bremen",         "hamburg": "Hamburg",
    "hesse": "Hesse",           "hessen": "Hesse",
    "mecklenburg-vorpommern": "Mecklenburg-Vorpommern",
    "lower saxony": "Lower Saxony",         "niedersachsen": "Lower Saxony",
    "north rhine-westphalia": "North Rhine-Westphalia",
    "nordrhein-westfalen": "North Rhine-Westphalia",
    "rhineland-palatinate": "Rhineland-Palatinate", "rheinland-pfalz": "Rhineland-Palatinate",
    "saarland": "Saarland",
    "saxony": "Saxony",         "sachsen": "Saxony",
    "saxony-anhalt": "Saxony-Anhalt",       "sachsen-anhalt": "Saxony-Anhalt",
    "schleswig-holstein": "Schleswig-Holstein",
    "thuringia": "Thuringia",   "thüringen": "Thuringia",       "thueringen": "Thuringia",
}


def norm_state(s):
    return DE_STATES.get((s or "").strip().lower(), (s or "").strip())


def clean_text(x):
    return re.sub(r"\s+", " ", x).strip() if x else ""


def safe_get(url, timeout=30):
    try:
        r = SESSION.get(url, timeout=timeout)
        r.raise_for_status()
        return r
    except Exception as e:
        tqdm.write(f"[WARN] Failed GET {url}: {e}")
        return None


def to_record(**kw):
    rec = {k: "" for k in CSV_FIELDS}
    rec["country"] = "Germany"
    rec.update(kw)
    rec["id"] = ""
    return rec


def _get_json_with_backoff(url, params, base_pause, debug=False):
    """GET JSON with jitter delay to be polite to Google Places."""
    time.sleep(base_pause + random.uniform(0, 0.6))
    try:
        resp = SESSION.get(url, params=params, timeout=30)
        resp.raise_for_status()
        return resp.json()
    except requests.exceptions.RequestException as e:
        if debug:
            print(f"[HTTP] {url} failed: {e}", file=sys.stderr)
        return {"status": "HTTP_ERROR", "error_message": str(e)}


# ─── Source 1: german-breweries.com ───────────────────────────────────────────
# The page at /all_breweries.htm is a single large HTML document listing
# ~1,559 breweries organised in per-state tables.  No separate detail pages
# exist — all data is inline.
#
# Table layout (4 columns, breweries listed left then right):
#   PLACE  |  BREWERY (+ optional map/tap links)  |  PLACE  |  BREWERY ...
#
# Brewery cells optionally contain a goyellow.de map URL that encodes the
# full postal address in the path:
#   http://www.goyellow.de/map/{postal5}-{city-slug}/{street-slug}/
#
# State headings appear as  <p>  elements immediately before each table,
# e.g.  BAYERN (675)  or  NORDRHEIN-WESTFALEN (148).

_BASE_GBC    = "http://www.german-breweries.com"
_GOYELLOW_RE = re.compile(
    r"/map/(\d{5})-([^/]+)/([^/]+)/?$"
)
_STATE_HDG_RE = re.compile(
    r"^([A-ZÄÖÜ][A-ZÄÖÜ\-]+(?:\s+[A-ZÄÖÜ][A-ZÄÖÜ\-]+)*)\s*\(\d+\)\s*$",
    re.UNICODE,
)


# German/common prepositions and articles that should stay lowercase in city names
_LOWERCASE_WORDS = frozenset([
    "an", "am", "auf", "bei", "der", "die", "das", "des", "dem", "den",
    "im", "in", "ob", "vor", "von", "zu", "zur", "über", "unter",
    "and", "the", "of",
])


def _title_slug(slug):
    """
    Convert a hyphenated ASCII slug to a readable name:
      "bad-neustadt-an-der-saale"  → "Bad Neustadt an der Saale"
      "willy-brandt-str.-8"        → "Willy-Brandt-Str. 8"
    Rules:
      - hyphens before a digit become a space ("str.-8" → "str. 8")
      - other hyphens: keep if the word looks like a compound ("willy-brandt"), else space
      - common prepositions / articles stay lowercase
    """
    # First: hyphens before digits → space (address number separator)
    slug = re.sub(r"-(\d)", r" \1", slug)
    # Split on spaces
    words = slug.split()
    result = []
    for i, word in enumerate(words):
        # Parts of a word joined by hyphens (compound noun)?
        parts = word.split("-")
        titled = "-".join(
            (p.capitalize() if p and (i == 0 or p.lower() not in _LOWERCASE_WORDS) else p)
            for p in parts
        )
        # Force first word to be capitalised
        if i == 0 and titled:
            titled = titled[0].upper() + titled[1:]
        # Lowercase prepositions in subsequent words
        elif titled.lower() in _LOWERCASE_WORDS:
            titled = titled.lower()
        result.append(titled)
    return " ".join(result)


def _decode_goyellow(url):
    """
    Parse  http://www.goyellow.de/map/73431-aalen/galgenbergstr.-8/
    → (postal_code='73431', city='Aalen', address_1='Galgenbergstr. 8')

    City slug:   ALL hyphens are word-separators (city names never hyphenate).
    Street slug: only hyphens immediately before a digit are separators
                 (the house number); other hyphens are part of compound names.
    Both use the common-preposition lowercase rule.
    """
    m = _GOYELLOW_RE.search(url)
    if not m:
        return "", "", ""

    postal      = m.group(1)
    city_slug   = m.group(2)   # e.g. "bad-neustadt-an-der-saale"
    street_slug = m.group(3)   # e.g. "galgenbergstr.-8"

    # City: replace every hyphen with a space, then title-case with prepositions
    city_words = city_slug.replace("-", " ").split()
    city = " ".join(
        (w.capitalize() if (i == 0 or w.lower() not in _LOWERCASE_WORDS) else w.lower())
        for i, w in enumerate(city_words)
    )

    # Street: only hyphen-before-digit → space (house number), rest stay
    street = _title_slug(street_slug)

    return postal, city, street


def _is_state_heading(text):
    """Return normalised state name if text looks like  BUNDESLAND (N),  else ''."""
    m = _STATE_HDG_RE.match(text.strip())
    if not m:
        return ""
    raw = m.group(1).strip()
    return norm_state(raw) or raw.title()


def scrape_german_breweries_com(limit=None, debug=False):
    """
    Parse the all-breweries listing from german-breweries.com inline.
    No separate HTTP requests per brewery — all data is on one page.

    Walks the document in order; state-heading <p> elements set the current
    Bundesland; PLACE/BREWERY table rows yield one record each.
    Address extracted from goyellow.de map URL when present.
    """
    index_url = f"{_BASE_GBC}/all_breweries.htm"
    r = safe_get(index_url)
    if not r:
        tqdm.write("[WARN] Could not fetch german-breweries.com — skipping source.")
        return []

    soup = BeautifulSoup(r.content, "html.parser", from_encoding="windows-1252")

    records     = []
    current_state = ""

    def _extract_row_half(city_td, brewery_td):
        """Extract one (city, brewery_name, website, postal, address) from a pair of TDs."""
        from bs4 import NavigableString

        city_text = clean_text(city_td.get_text(" ")) if city_td else ""
        if not city_text or city_text == "\xa0":
            city_text = ""

        # Brewery name from the first non-map/tap external link
        brewery_name = ""
        website      = ""
        postal       = ""
        address_1    = ""
        map_city     = ""

        has_taproom = False

        if brewery_td:
            for a in brewery_td.find_all("a", href=True):
                href      = a["href"].strip()
                link_text = clean_text(a.get_text(" "))

                if "goyellow" in href:
                    if link_text.lower() == "tap":
                        # A goyellow "tap" link means the brewery has a public taproom
                        has_taproom = True
                    elif not postal:
                        # First goyellow "map" link = brewery address
                        postal, map_city, address_1 = _decode_goyellow(href)
                elif link_text.lower() in ("map", "tap"):
                    pass  # rare non-goyellow map/tap links
                elif href.startswith("http") and not brewery_name:
                    brewery_name = link_text
                    website      = href

            # If brewery has no website link, its name is plain text in the TD.
            # Collect only NavigableString nodes that are NOT inside an <a> tag.
            if not brewery_name:
                parts = []
                for node in brewery_td.descendants:
                    if isinstance(node, NavigableString) and not node.find_parent("a"):
                        t = clean_text(str(node))
                        if t and t != "\xa0":
                            parts.append(t)
                brewery_name = clean_text(" ".join(parts))

        resolved_city = map_city or city_text  # prefer goyellow city (has better casing)

        return brewery_name, resolved_city, website, postal, address_1, has_taproom

    # ── Walk ALL top-level elements to track state headings between tables ──
    # The outer structure is a giant layout table so we can't simply iterate
    # siblings; instead find all <p> headings and all inner brewery tables and
    # sort them by document position.

    # Collect (document_order_index, element) for headings and brewery tables
    PLACE_HEADER_RE = re.compile(r"place|district", re.IGNORECASE)

    heading_els = []
    table_els   = []

    for i, el in enumerate(soup.find_all(True)):  # all tags, in order
        if el.name == "p":
            t = clean_text(el.get_text(" "))
            s = _is_state_heading(t)
            if s:
                heading_els.append((i, s))
        elif el.name == "table":
            # Only brewery tables — first row header must contain PLACE or DISTRICT
            first_row = el.find("tr")
            if first_row and PLACE_HEADER_RE.search(first_row.get_text()):
                table_els.append((i, el))

    if debug:
        print(f"[GBC] {len(heading_els)} state headings, {len(table_els)} brewery tables", file=sys.stderr)

    # Pair each table with the most recent preceding heading
    def state_for_table(tbl_idx):
        best = ""
        for h_idx, state in heading_els:
            if h_idx < tbl_idx:
                best = state
            else:
                break
        return best

    seen_keys = set()

    for tbl_idx, (doc_idx, table) in enumerate(table_els):
        state = state_for_table(doc_idx)
        rows  = table.find_all("tr")

        for tr in rows:
            tds = tr.find_all(["td", "th"])
            if len(tds) < 2:
                continue

            # Skip header rows
            row_text = clean_text(tr.get_text(" ")).lower()
            if "place" in row_text[:20] or "brewery" in row_text[:20]:
                continue
            if "district" in row_text[:20]:
                continue

            # 4-column layout: city | brewery | city | brewery
            pairs = []
            if len(tds) >= 4:
                pairs = [(tds[0], tds[1]), (tds[2], tds[3])]
            elif len(tds) >= 2:
                pairs = [(tds[0], tds[1])]

            for city_td, brewery_td in pairs:
                name, city, website, postal, addr, has_tap = _extract_row_half(city_td, brewery_td)
                if not name:
                    continue

                key = name.lower()
                if key in seen_keys:
                    continue
                seen_keys.add(key)

                # All entries on german-breweries.com are confirmed commercial breweries
                btype = classify_brewery_type(name, has_taproom=has_tap, default_micro=True)

                if debug:
                    print(
                        f"[GBC] {state}: {name!r} in {city!r} "
                        f"({postal}) type={btype!r} taproom={has_tap} "
                        f"addr={addr!r} web={website!r}",
                        file=sys.stderr,
                    )

                records.append(to_record(
                    name           = name,
                    brewery_type   = btype,
                    city           = city,
                    address_1      = addr,
                    postal_code    = postal,
                    state_province = state,
                    website_url    = website,
                ))

                if limit and len(records) >= limit:
                    return records

    return records


# ─── Source 2: Wikipedia ──────────────────────────────────────────────────────

# Map Wikipedia section keywords → OpenBreweryDB-style brewery_type
_SECTION_TYPE_MAP = {
    "major":       "large",
    "large":       "large",
    "regional":    "regional",
    "craft":       "micro",
    "micro":       "micro",
    "small":       "micro",
    "brewpub":     "brewpub",
    "independent": "regional",
    "notable":     "regional",
}


def _guess_type_from_heading(heading_text):
    ht = heading_text.lower()
    for kw, tag in _SECTION_TYPE_MAP.items():
        if kw in ht:
            return tag
    return "regional"


def scrape_wikipedia_de(limit=None, debug=False):
    """
    Parse wikitables from:
    https://en.wikipedia.org/wiki/List_of_brewing_companies_in_Germany

    NOTE: The EN Wikipedia page currently has ~10 major brewing companies
    (Oettinger, Krombacher, Beck's, Warsteiner, …) in a single table.
    It is included as a supplementary source for well-known brands;
    the main comprehensive dataset comes from german-breweries.com (~1,500).

    The parser is table-layout-agnostic and will adapt to any name/location/
    state/type columns present, so it will benefit if Wikipedia later expands.
    """
    url = "https://en.wikipedia.org/wiki/List_of_brewing_companies_in_Germany"
    r = safe_get(url)
    if not r:
        return []

    soup = BeautifulSoup(r.text, "html.parser")

    # ── helpers ──────────────────────────────────────────────────────────────

    def cell_text(el):
        for sup in el.select("sup.reference"):
            sup.decompose()
        return clean_text(el.get_text(" "))

    def norm_header(h):
        h = (h or "").lower()
        h = re.sub(r"[^a-z0-9]+", " ", h)
        return h.strip()

    def flatten_wikitable(table):
        """Flatten a wikitable respecting rowspan / colspan."""
        rows_el = table.find_all("tr")
        if not rows_el:
            return [], []
        header_cells = rows_el[0].find_all(["th", "td"])
        headers      = [cell_text(th) for th in header_cells]
        ncols        = len(headers)
        if ncols == 0:
            return [], []

        span_down = [0]    * ncols
        carry_val = [None] * ncols
        rows = []

        for tr in rows_el[1:]:
            cells = [None] * ncols
            # Carry forward rowspan values
            for i in range(ncols):
                if span_down[i] > 0:
                    cells[i]     = carry_val[i]
                    span_down[i] -= 1
            j = 0
            for td in tr.find_all(["td", "th"]):
                while j < ncols and cells[j] is not None:
                    j += 1
                text    = cell_text(td)
                colspan = int(td.get("colspan", "1") or "1")
                rowspan = int(td.get("rowspan", "1") or "1")
                for k in range(colspan):
                    if j + k < ncols:
                        cells[j + k] = text
                        if rowspan > 1:
                            span_down[j + k] = rowspan - 1
                            carry_val[j + k] = text
                j += colspan
            if any(x is not None and x != "" for x in cells):
                rows.append([c or "" for c in cells])

        return headers, rows

    def heading_before_table(table):
        """Return text of the nearest h2/h3/h4 preceding this table."""
        for prev in table.find_previous_siblings():
            n = getattr(prev, "name", None)
            if n in ("h2", "h3", "h4"):
                span = prev.find("span", class_="mw-headline")
                return (span.get_text(" ", strip=True) if span
                        else prev.get_text(" ", strip=True))
        return ""

    # ── walk every wikitable ──────────────────────────────────────────────────
    all_records  = []
    seen_names   = set()
    per_limit    = max(5, min(10, limit)) if limit else None

    for table in soup.select("table.wikitable"):
        headers, flat_rows = flatten_wikitable(table)
        if not headers or not flat_rows:
            continue

        headers_norm = [norm_header(h) for h in headers]

        def idx_for(*opts):
            for i, h in enumerate(headers_norm):
                if h in set(opts):
                    return i
            # partial match fallback
            for i, h in enumerate(headers_norm):
                if any(o in h for o in opts):
                    return i
            return -1

        idx_name  = idx_for("brewery", "brewery name", "name", "company", "company name")
        if idx_name < 0:
            if debug:
                print(f"[WIKI-DE] Skipping table (no name col). headers={headers}", file=sys.stderr)
            continue

        idx_loc   = idx_for("location", "city", "headquarters", "place", "town", "municipality")
        idx_state = idx_for("state", "land", "bundesland", "federal state", "region", "province")
        idx_type  = idx_for("type", "brewery type", "style", "category")
        idx_web   = idx_for("website", "url", "web")
        idx_year  = idx_for("founded", "established", "year")

        section_heading  = heading_before_table(table)
        brewery_type_tag = _guess_type_from_heading(section_heading)

        if debug:
            print(
                f"[WIKI-DE] Section '{section_heading}' → type={brewery_type_tag!r}, "
                f"rows={len(flat_rows)}, headers={headers}",
                file=sys.stderr,
            )

        taken = 0
        for row in flat_rows:
            name = clean_text(row[idx_name]) if idx_name >= 0 else ""
            if not name or name.lower() in ("brewery", "name", "company"):
                continue
            # Skip pure-number rows (year headings, etc.)
            if re.fullmatch(r"[\d\s,\.]+", name):
                continue

            loc   = clean_text(row[idx_loc])   if idx_loc   >= 0 else ""
            state = clean_text(row[idx_state]) if idx_state >= 0 else ""
            btype = clean_text(row[idx_type])  if idx_type  >= 0 else ""
            web   = clean_text(row[idx_web])   if idx_web   >= 0 else ""

            key = name.lower()
            if key in seen_names:
                continue
            seen_names.add(key)

            # Normalise state (Wikipedia uses full English names)
            state_norm = norm_state(state)

            # Disambiguate city vs state when only one location column exists.
            # Only replace city with state when norm_state actually recognises it
            # (i.e., it returns something different from the raw value).
            city = loc
            if not state_norm and loc:
                resolved = norm_state(loc)
                if resolved and resolved.lower() != loc.lower():
                    # loc was a known Bundesland name — record it as state only
                    state_norm = resolved
                    city = ""
                # else: it's a city name, leave state_norm empty

            all_records.append(to_record(
                name           = name,
                city           = city,
                state_province = state_norm,
                brewery_type   = btype or brewery_type_tag,
                website_url    = web,
            ))
            taken += 1
            if per_limit and taken >= per_limit:
                break

    if debug:
        print(f"[WIKI-DE] Total records: {len(all_records)}", file=sys.stderr)

    return all_records


# ─── Google Places enrichment ─────────────────────────────────────────────────

def enrich_with_places(rows, api_key, pause=3.0, test_limit=None, debug=False, strict_de=True):
    """
    For each record, look it up via Google Places FindPlace → TextSearch (fallback),
    then fetch full address_components + geometry from Details.

    strict_de behaviour
    -------------------
    • strict_de=True  (default): drop records where Google Places returns a
      *confirmed* non-DE country code (e.g. "US", "AU").  Records where the
      API fails or returns no address_components are KEPT — we can't confirm
      they're outside Germany and we'd rather keep scraped data than lose it.
    • strict_de=False: keep everything.

    If the first few Places calls all return REQUEST_DENIED / OVER_QUERY_LIMIT
    a loud warning is printed so you don't wait hours for no output.
    """
    if not api_key:
        raise ValueError("Google API key is required for Places enrichment.")

    base_find    = "https://maps.googleapis.com/maps/api/place/findplacefromtext/json"
    base_search  = "https://maps.googleapis.com/maps/api/place/textsearch/json"
    base_details = "https://maps.googleapis.com/maps/api/place/details/json"

    def make_query(r):
        bits = [r.get("name", "")]
        if r.get("city"):           bits.append(r["city"])
        if r.get("state_province"): bits.append(r["state_province"])
        bits.append("brewery Germany")
        return " ".join(b for b in bits if b).strip()

    def log(msg):
        if debug:
            print(msg, file=sys.stderr)

    # Early-warning counters — alert if the first N calls all fail
    _EARLY_CHECK  = 5
    _api_errors   = 0   # consecutive non-OK statuses in the first _EARLY_CHECK calls
    _api_miss     = 0   # no place_id found (both find + search miss)
    _country_drop = 0   # dropped by the strict_de filter

    out          = []
    seen_ids     = set()   # id(r) → already appended

    for i, r in enumerate(tqdm(rows, desc="Enriching via Google Places", unit="brewery"), 1):
        if test_limit and i > test_limit:
            break

        q = make_query(r)
        if not q:
            out.append(r)   # nothing to look up; preserve scraped data
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
            # Warn early so the user can abort instead of waiting hours for nothing
            if i <= _EARLY_CHECK:
                _api_errors += 1
                tqdm.write(
                    f"[WARN] Places API returned '{status}' for '{q}'. "
                    f"Check your API key has 'Places API' enabled in Google Cloud Console "
                    f"and billing is active."
                )
            if _api_errors >= _EARLY_CHECK:
                tqdm.write(
                    "[ERROR] First 5 Places calls all failed — aborting enrichment early.\n"
                    "        Re-run with --enrich none to get unenriched results.\n"
                    "        Or run with --diagnose-places to check your API key."
                )
                # Return whatever we've collected so far + all remaining rows unchanged
                for remaining in rows[i - 1:]:
                    if id(remaining) not in seen_ids:
                        out.append(remaining)
                        seen_ids.add(id(remaining))
                return out
            # Auth errors: skip the TextSearch fallback (it will also fail)
            # and keep the original scraped record unchanged.
            out.append(r)
            seen_ids.add(id(r))
            continue

        # ── 1b) Fallback: text search scoped to DE (only when ZERO_RESULTS) ──
        if not place_id:
            search_json = _get_json_with_backoff(
                base_search,
                {"query": q, "key": api_key, "region": "DE"},
                base_pause=pause, debug=debug,
            )
            if search_json.get("status") == "OK" and search_json.get("results"):
                place_id = search_json["results"][0]["place_id"]
            else:
                log(f"[MISS] No Places match for '{q}'")
                _api_miss += 1
                # Keep the original scraped record unchanged — don't discard it
                out.append(r)
                seen_ids.add(id(r))
                continue

        # ── 2) Place Details ───────────────────────────────────────────────
        det_json = _get_json_with_backoff(
            base_details,
            {
                "place_id": place_id,
                "fields":   "name,formatted_address,address_components,"
                            "international_phone_number,website,geometry/location,"
                            "business_status,types",
                "key":      api_key,
            },
            base_pause=pause, debug=debug,
        )
        result     = det_json.get("result") or {}
        components = result.get("address_components", [])
        parts      = {t: c for c in components for t in c.get("types", [])}

        # ── DE country filter ──────────────────────────────────────────────
        # Only drop records that are CONFIRMED to be outside Germany.
        # If address_components is empty (API failure / no data), keep the record.
        country_code = ""
        for c in components:
            if "country" in (c.get("types") or []):
                country_code = c.get("short_name") or ""
                break

        if strict_de and country_code and country_code.upper() != "DE":
            log(f"[FILTER] Excluding non-DE '{result.get('name')}' (country={country_code})")
            _country_drop += 1
            continue   # drop confirmed non-DE results

        # If no address_components at all (API returned nothing useful), keep
        # the original scraped record without overwriting any fields.
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
        state_province = parts.get("administrative_area_level_1", {}).get("long_name", "")
        postal_code    = parts.get("postal_code",     {}).get("long_name",  "")

        # ── Merge (only overwrite empty fields from scraping) ──────────────
        if address_1:      r["address_1"]      = address_1
        if address_2:      r["address_2"]      = address_2
        if city:           r["city"]           = city
        if state_province: r["state_province"] = state_province
        if postal_code:    r["postal_code"]    = postal_code
        r["country"]    = "Germany"
        r["name"]       = r.get("name") or result.get("name", "")
        r["phone"]      = r.get("phone")       or result.get("international_phone_number", "")
        r["website_url"]= r.get("website_url") or result.get("website", "")

        # ── brewery_type from Places business_status + types ───────────────
        # Always let Places override name-based type — it has ground truth.
        business_status = result.get("business_status", "")
        places_types    = result.get("types", [])
        places_btype    = _places_to_obdb_type(business_status, places_types)
        if places_btype:
            r["brewery_type"] = places_btype
            log(f"[TYPE] {r['name']!r}: Places→{places_btype!r} "
                f"(status={business_status!r}, types={places_types})")
        elif not r.get("brewery_type"):
            # No Places signal and no name-based type yet — apply name heuristic now
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
            f"[INFO] Enrichment summary: "
            f"{len(out)} kept, {_country_drop} dropped (non-DE), "
            f"{_api_miss} no-match (kept unchanged)"
        )

    # In non-strict mode, include any row we never processed (beyond test_limit, etc.)
    if not strict_de:
        for row in rows:
            if id(row) not in seen_ids:
                out.append(row)

    return out


# ─── Post-processing helpers ──────────────────────────────────────────────────

def filter_de_only(rows):
    """Keep only records explicitly tagged country=Germany."""
    out = []
    for r in rows:
        if r.get("country") == "Germany":
            out.append(r)
        elif r.get("address_1", "").lower().endswith("germany"):
            r["country"] = "Germany"
            out.append(r)
    return out


def dedupe(records):
    """Deduplicate by (name.lower(), city.lower())."""
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
        description="Scrape German breweries from german-breweries.com and Wikipedia, then export CSV."
    )
    ap.add_argument("-o", "--output",  required=True,
                    help="Output CSV file path.")
    ap.add_argument("--test",          action="store_true",
                    help="Test mode: scrape ~5 records per source and exit.")
    ap.add_argument("--debug",         action="store_true",
                    help="Print verbose debug logs to stderr.")
    ap.add_argument(
        "--only",
        choices=["german_breweries_com", "wikipedia"],
        help="Scrape only one source (useful for debugging)."
    )
    ap.add_argument(
        "--enrich", choices=["places", "none"], default="none",
        help="Enrich with Google Places API (adds address, phone, lat/lng). Default: none."
    )
    ap.add_argument("--google-api-key",
                    help="Google Places API key (required if --enrich places).")
    ap.add_argument("--places-rate", type=float, default=3.0,
                    help="Seconds to pause between Places API calls (default: 3.0).")
    ap.add_argument(
        "--strict-de",  action="store_true", default=True,
        help="Only keep breweries whose Google Place resolves to Germany (default: on)."
    )
    ap.add_argument(
        "--no-strict-de", dest="strict_de", action="store_false",
        help="Disable Germany-only filtering."
    )
    ap.add_argument(
        "--diagnose-places", action="store_true",
        help="Test the Places API with one brewery and print the raw response, then exit. "
             "Requires --google-api-key."
    )
    ap.add_argument(
        "--classify-existing", metavar="INPUT_CSV",
        help="Apply name-based brewery_type classification to an existing CSV and write "
             "updated rows to --output.  Does not re-scrape; only fills blank brewery_type "
             "cells using German name heuristics."
    )

    args  = ap.parse_args()
    limit = 5 if args.test else None

    # ── Quick API diagnostic (one call, then exit) ────────────────────────────
    if args.diagnose_places:
        if not args.google_api_key:
            raise SystemExit("ERROR: --google-api-key is required for --diagnose-places.")
        import json as _json
        key = args.google_api_key
        q   = "Löwenbrauerei München Bayern brewery Germany"
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
            r2 = SESSION.get(base_search, params={"query": q, "key": key, "region": "DE"}, timeout=30)
            j2 = r2.json()
            print("\n=== TextSearch fallback ===")
            print(_json.dumps(j2, indent=2, ensure_ascii=False)[:800])
            place_id = j2["results"][0]["place_id"] if j2.get("status") == "OK" and j2.get("results") else None

        if place_id:
            print(f"\nplace_id: {place_id}")
            r3 = SESSION.get(base_details, params={
                "place_id": place_id,
                "fields": "name,formatted_address,address_components,geometry/location",
                "key": key
            }, timeout=30)
            j3 = r3.json()
            print("\n=== Place Details ===")
            print(_json.dumps(j3, indent=2, ensure_ascii=False)[:1200])
            components = (j3.get("result") or {}).get("address_components", [])
            country = next((c.get("short_name") for c in components if "country" in c.get("types", [])), "(not found)")
            print(f"\n→ country_code: {country!r}")
        else:
            print("\nNo place_id found — Places API may not be enabled for this key.")
        raise SystemExit(0)

    # ── Classify existing CSV ─────────────────────────────────────────────────
    if args.classify_existing:
        with open(args.classify_existing, newline="", encoding="utf-8") as f:
            reader  = csv.DictReader(f)
            in_fields = reader.fieldnames or []
            rows    = list(reader)

        # Ensure address_3 and brewery_type columns exist in output
        out_fields = list(in_fields)
        if "address_3" not in out_fields:
            idx = out_fields.index("address_2") + 1 if "address_2" in out_fields else len(out_fields)
            out_fields.insert(idx, "address_3")
        if "brewery_type" not in out_fields:
            out_fields.insert(1, "brewery_type")

        changed = 0
        for row in rows:
            if not row.get("brewery_type"):
                # default_micro=True: every record here is a confirmed brewery
                t = classify_brewery_type(row.get("name", ""), default_micro=True)
                if t:
                    row["brewery_type"] = t
                    changed += 1
            if "address_3" not in row:
                row["address_3"] = ""

        with open(args.output, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=out_fields, extrasaction="ignore")
            writer.writeheader()
            writer.writerows(rows)

        tqdm.write(f"[DONE] {len(rows)} rows written → {args.output}  "
                   f"({changed} brewery_type values filled in)")
        raise SystemExit(0)

    # Sources: each callable accepts (limit) as positional arg
    sources_map = {
        "german_breweries_com": (
            "german-breweries.com",
            lambda lim: scrape_german_breweries_com(lim, debug=args.debug),
        ),
        "wikipedia": (
            "Wikipedia (List of brewing companies in Germany)",
            lambda lim: scrape_wikipedia_de(lim, debug=args.debug),
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

    tqdm.write(f"[INFO] Collected {len(records)} raw records")
    records = dedupe(records)
    tqdm.write(f"[INFO] After dedup: {len(records)} breweries")

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
            strict_de  = args.strict_de,
        )
        tqdm.write(f"[INFO] After Places enrichment: {len(records)} records")
        if args.strict_de:
            records = filter_de_only(records)
            tqdm.write(f"[INFO] After DE-only filter: {len(records)} records")

    write_csv(records, args.output)
    tqdm.write(f"[DONE] Written {len(records)} rows → {args.output}")

    if args.test:
        tqdm.write("[TEST MODE] Partial scrape complete.")


if __name__ == "__main__":
    main()
