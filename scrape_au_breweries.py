#!/usr/bin/env python3
# scrape_au_breweries.py
import argparse
import csv
import re
import sys
import time 
from urllib.parse import urlparse

import requests
from bs4 import BeautifulSoup
from tqdm import tqdm

from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

import random

CSV_FIELDS = [
    "id","name","brewery_type","address_1","address_2","city",
    "state_province","postal_code","country","phone","website_url",
    "longitude","latitude"
]

SESSION = requests.Session()
SESSION.headers.update({"User-Agent": "Mozilla/5.0 (BreweryScraper/1.2)"})

retry_cfg = Retry(
    total=6,
    connect=3,
    read=3,
    backoff_factor=0.7,                     # exponential backoff: 0.7, 1.4, 2.8, ...
    status_forcelist=(429, 500, 502, 503, 504),
    allowed_methods=("GET",),               # urllib3>=1.26; for older: method_whitelist
    respect_retry_after_header=True,
    raise_on_status=False,
)
adapter = HTTPAdapter(max_retries=retry_cfg, pool_maxsize=10)
SESSION.mount("https://", adapter)
SESSION.mount("http://", adapter)

AU_STATES = {
    "nsw": "New South Wales", "new south wales": "New South Wales",
    "vic": "Victoria", "victoria": "Victoria",
    "qld": "Queensland", "queensland": "Queensland",
    "wa": "Western Australia", "western australia": "Western Australia",
    "sa": "South Australia", "south australia": "South Australia",
    "tas": "Tasmania", "tasmania": "Tasmania",
    "act": "Australian Capital Territory", "australian capital territory": "Australian Capital Territory",
    "nt": "Northern Territory", "northern territory": "Northern Territory"
}

def norm_state(s): return AU_STATES.get(s.strip().lower(), s.title()) if s else ""
def clean_text(x): return re.sub(r"\s+", " ", x).strip() if x else ""

def safe_get(url):
    try:
        r = SESSION.get(url, timeout=30)
        r.raise_for_status()
        return r
    except Exception as e:
        tqdm.write(f"[WARN] Failed GET {url}: {e}")
        return None

def to_record(**kw):
    rec = {k: "" for k in CSV_FIELDS}
    rec.update(kw)
    rec["id"] = ""
    # DO NOT force country here; we’ll set it after AU check in enrichment
    return rec


def _get_json_with_backoff(url, params, base_pause, debug=False):
    """
    GET JSON with retries handled by SESSION + our own jitter between calls.
    """
    # jitter to avoid thundering herd and rate spikes
    time.sleep(base_pause + random.uniform(0, 0.6))

    try:
        resp = SESSION.get(url, params=params, timeout=30)
        resp.raise_for_status()
        return resp.json()
    except requests.exceptions.RequestException as e:
        if debug:
            print(f"[HTTP] {url} failed: {e}", file=sys.stderr)
        return {"status": "HTTP_ERROR", "error_message": str(e)}

# ---------------- Wikipedia ----------------
def scrape_wikipedia(limit=None, debug=False):
    """
    Scrape the two wikitables:
      - 'Breweries owned by major companies'
      - 'Microbreweries'

    Extract:
      - Brewery name (→ name)
      - Location(s) (→ address_1, to be enriched later)
    """
    url = "https://en.wikipedia.org/wiki/List_of_breweries_in_Australia"
    r = safe_get(url)
    if not r:
        return []

    soup = BeautifulSoup(r.text, "html.parser")

    # ---------- helpers ----------
    def cell_text(el):
        for sup in el.select("sup.reference"):
            sup.decompose()
        return clean_text(el.get_text(" "))

    def norm_header(h):
        h = (h or "").lower()
        h = re.sub(r"[^a-z0-9]+", " ", h)
        return h.strip()

    def heading_node_by_id(soup, section_id):
        anchor = soup.find(id=section_id)
        if anchor:
            h = anchor.find_parent(["h2", "h3"])
            if h:
                return h
        want = section_id.replace("_", " ").strip().lower()
        for h in soup.select("h2, h3"):
            span = h.find("span", class_="mw-headline")
            text = (span.get_text(" ", strip=True) if span else h.get_text(" ", strip=True)).lower()
            if text == want:
                return h
        return None

    def first_wikitable_after(hnode):
        for el in hnode.next_elements:
            if getattr(el, "name", None) == "table" and "wikitable" in (el.get("class") or []):
                return el
        return None

    def flatten_wikitable(table):
        header_cells = table.find("tr").find_all(["th", "td"])
        headers = [cell_text(th) for th in header_cells]
        ncols = len(headers)
        span_down = [0] * ncols
        carry_val = [None] * ncols
        rows = []
        for tr in table.find_all("tr")[1:]:
            cells = [None] * ncols
            for i in range(ncols):
                if span_down[i] > 0:
                    cells[i] = carry_val[i]
                    span_down[i] -= 1
            j = 0
            for td in tr.find_all(["td", "th"]):
                while j < ncols and cells[j] is not None:
                    j += 1
                text = cell_text(td)
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

    def build_records_from_table(table, brewery_type_tag, limit_each=None):
        headers, flat_rows = flatten_wikitable(table)
        headers_norm = [norm_header(h) for h in headers]

        def idx_for(opts):
            for i, h in enumerate(headers_norm):
                if h in opts:
                    return i
            return -1

        idx_brewery = idx_for({"brewery", "brewery name", "name"})
        idx_location = idx_for({"location s", "location", "locations"})

        if idx_brewery < 0 or idx_location < 0:
            if debug:
                print(f"[WIKI] Missing expected cols. Headers: {headers}", file=sys.stderr)
            return []

        out = []
        taken = 0
        for row in flat_rows:
            name = clean_text(row[idx_brewery])
            loc = clean_text(row[idx_location])
            if not name or name.lower() == "brewery":
                continue
            if re.fullmatch(r"\d{3,4}", name):  # skip year rows
                continue

            out.append(to_record(
                name=name,
                address_1=loc,
                brewery_type=brewery_type_tag
            ))
            taken += 1
            if limit_each and taken >= limit_each:
                break
        return out

    # ---------- main logic ----------
    targets = [
        ("Breweries_owned_by_major_companies", "major_company_owned"),
        ("Microbreweries", "microbrewery"),
    ]

    all_records = []  # ✅ define here
    per_table_limit = max(5, min(10, limit)) if limit else None  # ✅ define here

    for section_id, tag in targets:
        h = heading_node_by_id(soup, section_id)
        if not h:
            if debug:
                print(f"[WIKI] No heading for {section_id}", file=sys.stderr)
            continue
        tbl = first_wikitable_after(h)
        if not tbl:
            if debug:
                print(f"[WIKI] No wikitable after {section_id}", file=sys.stderr)
            continue

        recs = build_records_from_table(tbl, tag, per_table_limit)
        if debug:
            print(f"[WIKI] {section_id}: {len(recs)} rows", file=sys.stderr)
        all_records.extend(recs)

    return all_records



# ---------------- Craft Cartel ----------------
def scrape_craftcartel(limit=None):
    """
    Extract the single long comma-separated list of brewery names from:
      https://craftcartel.com.au/the-a-to-z-of-australian-craft-breweries/

    Output rows only contain:
      name (from the list), country=Australia
    All other fields left blank per schema.
    """
    url = "https://craftcartel.com.au/the-a-to-z-of-australian-craft-breweries/"
    r = safe_get(url)
    if not r:
        return []

    soup = BeautifulSoup(r.text, "html.parser")

    # Search likely content areas
    candidates = soup.select("article p, .entry-content p, .post-content p, p")

    list_para = None
    best_score = -1

    for p in candidates:
        text = clean_text(p.get_text(" "))
        if not text or len(text) < 10:
            continue

        # Heuristic: the brewery list paragraph has LOTS of commas.
        comma_count = text.count(",")
        token_count = len(text.split())
        # Penalise paragraphs that mention obvious non-list sections
        bad_phrases = [
            "subscription", "beer boxes", "delivery", "history of brewing",
            "first australian brewery", "craft beer", "gift pack", "allow 7 working days"
        ]
        penalty = sum(1 for bp in bad_phrases if bp in text.lower())

        # Prefer paragraphs where many tokens are comma-separated names & few full stops.
        score = comma_count - penalty*10 - text.count(".")

        # Require at least a decent number of commas (e.g., > 20) to be considered the list
        if comma_count > 20 and score > best_score:
            best_score = score
            list_para = text

    if not list_para:
        tqdm.write("[WARN] Could not find the Craft Cartel A–Z list paragraph.")
        return []

    # Split the monster paragraph into names
    raw_items = [i.strip() for i in list_para.split(",")]

    # Clean each name
    def clean_name(n: str) -> str:
        # Remove leading/trailing quotes or stray punctuation
        n = re.sub(r'^[“"\'\s]+|[”"\'\s]+$', "", n)
        # Collapse whitespace
        n = clean_text(n)
        # Weed out obvious non-brewery fragments
        bad_bits = [
            "look no further", "the perfect way", "for sydney", "allow 7 working days",
            "check out our list", "the classic craft box", "our favourite mixed box",
            "the list goes on", "ginger beer", "kombucha", "seltzer", "non-alcoholic",
            "a great way", "package", "delivered straight to your door"
        ]
        if any(b in n.lower() for b in bad_bits):
            return ""
        # Very short tokens are probably noise
        if len(n) < 2:
            return ""
        return n

    names = []
    seen = set()
    for item in raw_items:
        nm = clean_name(item)
        if not nm:
            continue
        key = nm.lower()
        if key in seen:
            continue
        seen.add(key)
        names.append(nm)
        if limit and len(names) >= limit:
            break

    # Convert to records
    records = [
        to_record(name=n)  # country defaults to Australia in to_record()
        for n in names
    ]
    return records


# ---------------- Independent Brewers Australia ----------------
def scrape_iba(limit=None, debug=False):
    """
    Crawl IBA Brewery Members across paginated pages.
    Example pages:
      /brewery-members/
      /brewery-members/page/2/
      ...
    Extracts:
      - name  from <h3.x-text-content-text-primary>
      - city/state from nearby div like 'ALEXANDRIA NSW'
    """
    import itertools

    BASE = "https://independentbrewers.org.au/brewery-members/"
    MAX_PAGES_DEFAULT = 50  # hard safety cap

    # In --test mode, don't crawl all pages
    max_pages = 2 if limit else MAX_PAGES_DEFAULT

    def fetch(page: int):
        url = BASE if page == 1 else f"{BASE}page/{page}/"
        resp = safe_get(url)
        if debug:
            print(f"[IBA] GET {url} -> {resp.status_code if resp else 'ERR'}", file=sys.stderr)
        if not resp:
            return None, url
        return BeautifulSoup(resp.text, "html.parser"), url

    # Regex for "CITY STATE" or "CITY, STATE"
    loc_re = re.compile(
        r"^\s*([A-Za-z][A-Za-z\s\-\.'’&/]+?)[,\s]+(NSW|VIC|QLD|WA|SA|TAS|ACT|NT)\s*$",
        re.IGNORECASE,
    )

    def clean_city(s: str) -> str:
        # Keep punctuation like hyphens/apostrophes, but Title Case words
        parts = re.split(r"(\s|-)", s.strip())
        return "".join(p.capitalize() if p.isalpha() else p for p in parts)

    SKIP_KEYWORDS = {"want to be a member", "brewery members", "become a member"}

    def extract_from_soup(soup: "BeautifulSoup"):
        # Heuristic container: the grid that holds member cards can change; focus on the h3s
        name_nodes = soup.select("h3.x-text-content-text-primary")
        if not name_nodes:
            # fallback if class name shifts slightly: any h3 with similar class fragment
            name_nodes = [h for h in soup.select("h3") if any("x-text-content-text-primary" in c for c in h.get("class", []))]

        records = []
        for h3 in name_nodes:
            name = clean_text(h3.get_text(" "))
            if not name:
                continue
            if any(k in name.lower() for k in SKIP_KEYWORDS):
                if debug:
                    print(f"[IBA] Skip non-brewery header: {name}", file=sys.stderr)
                continue

            # Look for a nearby div containing SUBURB STATE
            m = None
            # Check a few siblings
            for sib in itertools.islice(h3.next_siblings, 0, 6):
                if getattr(sib, "get_text", None):
                    t = clean_text(sib.get_text(" "))
                    if not t:
                        continue
                    m = loc_re.match(t)
                    if m:
                        break
                if getattr(sib, "select", None):
                    for div in sib.select("div"):
                        t = clean_text(div.get_text(" "))
                        if not t:
                            continue
                        m = loc_re.match(t)
                        if m:
                            break
                if m:
                    break
            # If not found, try within the enclosing card container
            if not m:
                card = h3.find_parent(["div", "section", "article"])
                if card:
                    for div in itertools.islice(card.find_all("div", recursive=True), 0, 24):
                        t = clean_text(div.get_text(" "))
                        if not t:
                            continue
                        m = loc_re.match(t)
                        if m:
                            break

            city, state = "", ""
            if m:
                city = clean_city(m.group(1))
                state = m.group(2).upper()

            # If name is a link, capture it as a tentative website
            website = ""
            a = h3.find("a", href=True)
            if a and a["href"].startswith("http"):
                website = a["href"]

            records.append(to_record(
                name=name,
                city=city,
                state_province=state,
                website_url=website
            ))
        return records

    out = []
    seen = set()

    # Crawl pages
    page = 1
    pages_seen = 0
    while pages_seen < max_pages:
        soup, url = fetch(page)
        if not soup:
            if debug:
                print(f"[IBA] Stop: fetch failed at page {page}", file=sys.stderr)
            break

        page_records = extract_from_soup(soup)
        if debug:
            print(f"[IBA] Page {page}: {len(page_records)} raw records", file=sys.stderr)

        # If a page yields nothing, assume we're past the last page
        if not page_records and page > 1:
            if debug:
                print(f"[IBA] Stop: empty page at {url}", file=sys.stderr)
            break

        # Deduplicate by (name,state)
        added = 0
        for r in page_records:
            key = (r["name"].lower(), r["state_province"].lower())
            if key in seen:
                continue
            seen.add(key)
            out.append(r)
            added += 1
            # Respect overall limit (used by --test)
            if limit and len(out) >= limit:
                if debug:
                    print(f"[IBA] Limit reached ({limit}), stopping crawl", file=sys.stderr)
                return out

        if debug:
            print(f"[IBA] Page {page}: {added} added, total {len(out)}", file=sys.stderr)

        # Try to discover last page from pager (if present)
        # If pager exists, prefer rel="next"; else increment until empty
        next_link = soup.select_one("a.next, a[rel='next'], nav.pagination a.next, .page-numbers a.next")
        pages_seen += 1

        if next_link:
            page += 1
            continue

        # No explicit next link; continue incrementally but stop if next page is empty later
        page += 1

    return out


def enrich_with_places(rows, api_key, pause=3.0, test_limit=None, debug=False, strict_au=True):
    import time

    if not api_key:
        raise ValueError("Google API key is required for Places enrichment.")

    base_find = "https://maps.googleapis.com/maps/api/place/findplacefromtext/json"
    base_search = "https://maps.googleapis.com/maps/api/place/textsearch/json"
    base_details = "https://maps.googleapis.com/maps/api/place/details/json"

    def make_query(r):
        bits = [r.get("name","")]
        if r.get("city"): bits.append(r["city"])
        if r.get("state_province"): bits.append(r["state_province"])
        bits.append("brewery Australia")
        return " ".join([b for b in bits if b]).strip()

    def log(msg):
        if debug:
            print(msg, file=sys.stderr)

    out = []
    for i, r in enumerate(rows, 1):
        if test_limit and i > test_limit:
            break  # stop enriching beyond test_limit

        q = make_query(r)
        if not q:
            continue

        # ---------- 1) Find Place ----------
        time.sleep(pause)
        find_resp = SESSION.get(base_find, params={
            "input": q,
            "inputtype": "textquery",
            "fields": "place_id,name",
            "key": api_key
        }, timeout=30)
        find_json = find_resp.json()
        status = find_json.get("status")
        if status != "OK":
            if status == "ZERO_RESULTS":
                # fallback to text search
                time.sleep(pause)
                search_resp = SESSION.get(base_search, params={
                    "query": q, "key": api_key, "region": "AU"
                }, timeout=30)
                search_json = search_resp.json()
                if search_json.get("status") == "OK" and search_json.get("results"):
                    place_id = search_json["results"][0]["place_id"]
                else:
                    log(f"[MISS] No match for '{q}'")
                    # No AU-verifiable data; skip in strict mode
                    if not strict_au:
                        out.append(r)
                    continue
            else:
                # REQUEST_DENIED etc handled earlier; just skip this row
                log(f"[FIND] Non-OK status={status} for '{q}'")
                if not strict_au:
                    out.append(r)
                continue
        else:
            place_id = (find_json.get("candidates") or [{}])[0].get("place_id")
            if not place_id:
                log(f"[MISS] No place_id for '{q}'")
                if not strict_au:
                    out.append(r)
                continue

        # ---------- 2) Place Details (ask for address_components) ----------
        time.sleep(pause)
        
        find_json = _get_json_with_backoff(base_find, {
            "input": q,
            "inputtype": "textquery",
            "fields": "place_id,name",
            "key": api_key
        }, base_pause=pause, debug=debug)

        # fallback if needed
        search_json = _get_json_with_backoff(base_search, {
            "query": q, "key": api_key, "region": "AU"
        }, base_pause=pause, debug=debug)

        det_json = _get_json_with_backoff(base_details, {
            "place_id": place_id,
            "fields": "name,formatted_address,address_components,international_phone_number,website,geometry/location",
            "key": api_key
        }, base_pause=pause, debug=debug)

        result = det_json.get("result") or {}

        # ---------- Parse address components ----------
        components = result.get("address_components", [])
        parts = {t: c for c in components for t in c.get("types", [])}

        # Street address
        street_number = parts.get("street_number", {}).get("long_name", "")
        route = parts.get("route", {}).get("long_name", "")
        address_1 = " ".join(x for x in [street_number, route] if x)

        # Subpremise (e.g. unit/apartment)
        address_2 = parts.get("subpremise", {}).get("long_name", "")

        # City / locality
        city = (
            parts.get("locality", {}).get("long_name")
            or parts.get("postal_town", {}).get("long_name")
            or parts.get("administrative_area_level_2", {}).get("long_name")
            or ""
        )

        # State
        state_province = parts.get("administrative_area_level_1", {}).get("short_name", "")

        # Postal code
        postal_code = parts.get("postal_code", {}).get("long_name", "")

        # Country
        country = parts.get("country", {}).get("long_name", "")

        # ---------- Assign back into the record ----------
        r["address_1"] = address_1
        r["address_2"] = address_2
        r["city"] = city
        r["state_province"] = state_province
        r["postal_code"] = postal_code
        r["country"] = country or "Australia"  # country check already done above


        # ---------- AU filter ----------
        comps = result.get("address_components") or []
        country_code = ""
        for c in comps:
            if "country" in (c.get("types") or []):
                country_code = c.get("short_name") or ""
                break

        if strict_au and country_code.upper() != "AU":
            log(f"[FILTER] Excluding non-AU '{result.get('name')}' country={country_code}")
            continue  # DROP the record

        # ---------- Merge ----------
        r["name"] = r.get("name") or result.get("name","")
        
        r["phone"] = r.get("phone") or result.get("international_phone_number","")
        r["website_url"] = r.get("website_url") or result.get("website","")
        loc = (result.get("geometry") or {}).get("location") or {}
        if not r.get("latitude") and loc.get("lat") is not None:
            r["latitude"] = str(loc["lat"])
        if not r.get("longitude") and loc.get("lng") is not None:
            r["longitude"] = str(loc["lng"])
        r["country"] = "Australia"  # now we know it’s AU

        out.append(r)

    # In strict AU mode, we only kept AU-verified rows above.
    # If not strict, include untouched rows (best effort).
    if not strict_au:
        # Add any rows we skipped during enrichment loop
        # (those beyond test_limit or without queries)
        enriched_ids = set(id(x) for x in out)
        for r in rows:
            if id(r) not in enriched_ids:
                out.append(r)

    return out

def filter_au_only(rows):
    out = []
    for r in rows:
        # keep if we explicitly set AU during enrichment
        if r.get("country") == "Australia":
            out.append(r)
        # or if address string clearly ends with Australia
        elif r.get("address_1","").lower().endswith("australia"):
            r["country"] = "Australia"
            out.append(r)
    return out


# ---------------- Dedup ----------------
def dedupe(records):
    seen, out = set(), []
    for r in tqdm(records, desc="Deduping records", unit="rec"):
        key = (r["name"].lower(), r["state_province"].lower())
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

def main():
    ap = argparse.ArgumentParser(description="Scrape Australian breweries and export CSV.")
    ap.add_argument("-o", "--output", required=True, help="Output CSV file path.")
    ap.add_argument("--test", action="store_true", help="Test mode: scrape a handful and exit.")
    ap.add_argument("--debug", action="store_true", help="Verbose debug logs for enrichment.")

    ap.add_argument(
        "--only",
        choices=["wikipedia", "craft_cartel", "iba"],
        help="Scrape only a single source (for debugging)."
    )
    ap.add_argument("--enrich", choices=["places", "none"], default="none",
                help="Enrich rows with address/phone/website/lat/lng. Uses Google Places if selected.")
    ap.add_argument("--google-api-key", help="Google Places API key (required if --enrich places).")
    ap.add_argument("--places-rate", type=float, default=3.0,
                help="Seconds to sleep between Places lookups to be polite and avoid rate caps (default: 3.0).")
    ap.add_argument(
        "--strict-au",
        action="store_true",
        default=True,
        help="Only include breweries whose Google Place is in Australia (default: on)."
    )
    ap.add_argument(
        "--no-strict-au",
        dest="strict_au",
        action="store_false",
        help="Disable AU-only filtering (keeps non-AU results)."
    )

    args = ap.parse_args()

    limit = 5 if args.test else None
    sources_map = {
        "wikipedia": ("Wikipedia", scrape_wikipedia),
        "craft_cartel": ("Craft Cartel", scrape_craftcartel),
        "iba": ("Independent Brewers Australia", scrape_iba)
    }

    if args.only:
        sources = [sources_map[args.only]]
    else:
        sources = list(sources_map.values())
    
                
    records = []
    for name, func in tqdm(sources, total=len(sources), desc="Scraping sources", unit="source"):
        recs = []
        try:
            recs = func(limit)
        except Exception as e:
            tqdm.write(f"[WARN] {name} scraping error: {e}")
        records += recs
        tqdm.write(f"[INFO] {name}: {len(recs)} found (running total: {len(records)})")

    tqdm.write(f"[INFO] Collected {len(records)} raw records")
    records = dedupe(records)
    tqdm.write(f"[INFO] Deduped to {len(records)} breweries")

    # After: records = dedupe(records)

    if args.enrich == "places":
        if not args.google_api_key:
            raise SystemExit("ERROR: --google-api-key is required when --enrich places is set.")
        tqdm.write("[INFO] Enriching with Google Places…")
        # In test mode, just enrich the first N rows so it’s quick
        test_limit = 20 if args.test else None
        records = enrich_with_places(
            records,
            args.google_api_key,
            pause=args.places_rate,
            test_limit=(20 if args.test else None),
            debug=args.debug,
            strict_au=args.strict_au,
        )

        tqdm.write(f"[INFO] After Places enrichment and AU filtering: {len(records)} records")
        records = filter_au_only(records) if args.strict_au else records

    write_csv(records, args.output)
    tqdm.write(f"[DONE] CSV written to {args.output}")

    if args.test:
        tqdm.write("[TEST MODE] Completed partial scrape and exited early.")

if __name__ == "__main__":
    main()
