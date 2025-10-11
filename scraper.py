import os, pathlib, hashlib, re, json, collections
from datetime import datetime
from bs4 import BeautifulSoup
from playwright.async_api import async_playwright, TimeoutError
import pandas as pd

# ===================== CONFIG =====================
BASE = "https://bis.gov.lv"
# Explicitly match the UI sort: case number DESC (stable long list)
LIST_URL = BASE + "/bisp/lv/planned_constructions/list?order=case_number&direction=desc&page={page}"

# How many list pages to scan this run (override in workflow env)
PAGES_TOTAL = int(os.getenv("PAGES_TOTAL", "5000"))

# Politeness delay between pages in milliseconds (0 = none)
PAGE_DELAY_MS = int(os.getenv("PAGE_DELAY_MS", "25"))

# Stop only after this many consecutive empty pages (network hiccups or gaps)
EMPTY_PAGE_TOLERANCE = int(os.getenv("EMPTY_PAGE_TOLERANCE", "3"))

# Folders/files
ROOT = pathlib.Path(".")
DEBUG_DIR = ROOT / "debug"; DEBUG_DIR.mkdir(exist_ok=True)
REPORTS = ROOT / "reports"; REPORTS.mkdir(parents=True, exist_ok=True)
STATE_DIR = ROOT / "state"; STATE_DIR.mkdir(parents=True, exist_ok=True)
STATE_FILE = STATE_DIR / "state.json"

# Your filters (we normalize values before comparing)
AUTHORITIES_WHITELIST = {
    "RĪGAS VALSTSPILSĪTAS PAŠVALDĪBAS PILSĒTAS ATTĪSTĪBAS DEPARTAMENTS",
    "Ādažu novada būvvalde",
    "Saulkrastu novada būvvalde",
    "Ropažu novada pašvaldības būvvalde",
    "Siguldas novada būvvalde",
    "Salaspils novada pašvaldības iestāde \"Salaspils novada Būvvalde\"",
    "Ogres novada pašvaldības centrālās administrācijas Ogres novada būvvalde",
    "Ķekavas novada pašvaldības būvvalde",
    "OLAINES NOVADA PAŠVALDĪBAS BŪVVALDE",
    "Mārupes novada Būvvalde",
    "Jūrmalas Būvvalde",
}

PHASE_KEEP = {
    "Iecere",
    "Būvniecības ieceres publiskā apspriešana",
    "Projektēšanas nosacījumu izpilde",
    "Būvdarbu uzsākšanas nosacījumu izpilde",
}

TYPE_KEEP = {
    "Atjaunošana",
    "Vienkāršota atjaunošana",
    "Jauna būvniecība",
    "Pārbūve",
    "Vienkāršota pārbūve",
}

# Column labels seen on /list?page=N
HEADER_MAP = {
    "Būvniecības kontroles institūcija": "authority",
    "Lietas numurs": "bis_number",
    "Būves nosaukums": "object",
    "Adrese": "address",
    "Būvniecības veids": "construction_type",
    "Būvniecības lietas stadija": "phase",
}
# ===================================================


# ===================== NORMALIZATION =====================
NBSP = "\u00A0"
def norm(s: str) -> str:
    if s is None: return ""
    s = s.replace(NBSP, " ")
    s = re.sub(r"\s+", " ", s)
    return s.strip()

AUTHORITIES_NORM = {norm(a): a for a in AUTHORITIES_WHITELIST}
PHASE_KEEP_NORM   = {norm(x) for x in PHASE_KEEP}
TYPE_KEEP_NORM    = {norm(x) for x in TYPE_KEEP}
# ========================================================


# ===================== PARSING =====================
def extract_value(cell, header_text: str) -> str:
    """
    Values include a screen-reader prefix 'Label: Value'. Strip 'Label:'.
    """
    val_el = cell.select_one(".flextable__value")
    t = norm(val_el.get_text(" ", strip=True) if val_el else "")
    prefix = header_text + ":"
    if t.startswith(prefix):
        t = norm(t[len(prefix):])
    return t

def parse_page(html: str):
    """
    Return (total_rows_on_page, parsed_rows, diag Counter by authority).
    'parsed_rows' are NOT filtered yet.
    """
    soup = BeautifulSoup(html, "lxml")
    row_nodes = soup.select(".flextable__row")
    total_rows = len(row_nodes)
    out = []
    diag = collections.Counter()

    for row in row_nodes:
        rec = {"details_url": None}
        for cell in row.select(".flextable__cell"):
            header = norm(cell.get("data-column-header-name") or "")
            key = HEADER_MAP.get(header)
            if not key:
                continue

            text = extract_value(cell, header)
            a = cell.select_one("a.public_list__link[href]")
            if key == "bis_number" and a:
                href = a.get("href", "")
                if href.startswith("/"):
                    href = BASE + href
                rec["details_url"] = href
                text = norm(a.get_text(" ", strip=True))
            rec[key] = text

        if rec.get("authority"):
            diag[norm(rec["authority"])] += 1

        # Make a stable key; prefer BIS number
        rec["_key"] = rec.get("bis_number") or hashlib.sha256(
            "|".join([rec.get("authority",""), rec.get("address",""), rec.get("object","")]).encode("utf-8")
        ).hexdigest()[:24]

        out.append(rec)

    return total_rows, out, diag
# ===================================================


# ===================== FILTER + DIFF =====================
def filter_row(rec: dict) -> bool:
    auth_n = norm(rec.get("authority"))
    if auth_n not in AUTHORITIES_NORM:
        return False
    phase_n = norm(rec.get("phase"))
    if phase_n and phase_n not in PHASE_KEEP_NORM:
        return False
    type_n = norm(rec.get("construction_type"))
    if type_n and type_n not in TYPE_KEEP_NORM:
        return False
    return True

def load_state() -> dict:
    if STATE_FILE.exists():
        return json.loads(STATE_FILE.read_text(encoding="utf-8"))
    return {}

def save_state(state: dict):
    STATE_FILE.write_text(json.dumps(state, ensure_ascii=False, indent=2), encoding="utf-8")

def compute_delta(prev_state: dict, filtered_rows: list[dict]):
    """
    Returns (delta_rows, updated_state, baseline_flag)
    - delta_rows: only NEW or CHANGED (phase) rows to report
    - updated_state: merged state after this run
    - baseline_flag: True if this is the very first run (no previous state)
    """
    baseline = (len(prev_state) == 0)
    today = datetime.now().strftime("%Y-%m-%d")
    updated = dict(prev_state)
    delta = []

    for r in filtered_rows:
        key = r["_key"]
        auth_n = norm(r.get("authority"))
        phase_n = norm(r.get("phase"))
        type_n  = norm(r.get("construction_type"))

        canon_authority = AUTHORITIES_NORM.get(auth_n, r.get("authority"))
        current = {
            "bis_number": r.get("bis_number",""),
            "authority": canon_authority or "",
            "address": r.get("address",""),
            "object": r.get("object",""),
            "phase": phase_n,
            "construction_type": type_n,
            "details_url": r.get("details_url",""),
            "last_seen": today,
        }

        old = updated.get(key)
        if old is None:
            if not baseline:
                delta.append({**current, "_key": key, "_change": "Jauns"})
            updated[key] = {**current, "first_seen": today}
        else:
            if old.get("phase") != phase_n:
                tag = f"Stadija: {old.get('phase','?')} → {phase_n or '?'}"
                delta.append({**current, "_key": key, "_change": tag})
            updated[key] = {
                **old,
                **current,
                "first_seen": old.get("first_seen", today)
            }

    return delta, updated, baseline
# =========================================================


# ===================== REPORTS (HTML) =====================
def html_table_from_rows(rows: list[dict], include_change_col: bool) -> str:
    if not rows:
        return "<p>Nav datu.</p>"
    cols = ["bis_number","authority","address","object","phase","construction_type","details_url"]
    if include_change_col:
        cols = ["_change"] + cols
    df = pd.DataFrame(rows)[cols].copy()

    def linkify(r):
        if r.get("details_url") and r.get("bis_number"):
            return f'<a href="{r["details_url"]}" target="_blank" rel="noopener">{r["bis_number"]}</a>'
        return r.get("bis_number","")

    df["BIS lieta"] = df.apply(linkify, axis=1)

    rename = {
        "authority": "Būvniecības kontroles institūcija",
        "address": "Adrese",
        "object": "Būves nosaukums",
        "phase": "Būvniecības lietas stadija",
        "construction_type": "Būvniecības veids",
        "_change": "Izmaiņas",
    }

    if include_change_col:
        df = df[["Izmaiņas","BIS lieta","Būvniecības kontroles institūcija","Adrese","Būves nosaukums",
                 "Būvniecības lietas stadija","Būvniecības veids"]].rename(columns={}, inplace=False)
    else:
        df = df[["BIS lieta","Būvniecības kontroles institūcija","Adrese","Būves nosaukums",
                 "Būvniecības lietas stadija","Būvniecības veids"]].rename(columns={}, inplace=False)

    df.rename(columns=rename, inplace=True, errors="ignore")
    return df.to_html(index=False, escape=False)

def wrap_html(title: str, body_html: str, pages_seen: int, scanned: int, extra_note: str = "") -> str:
    css = """
    <style>
      body{font-family:system-ui,-apple-system,Segoe UI,Roboto,Arial,sans-serif; padding:16px;}
      table{border-collapse:collapse; width:100%; font-size:14px;}
      th,td{border:1px solid #e5e7eb; padding:8px; vertical-align:top;}
      th{background:#f3f4f6; text-align:left;}
      a{ text-decoration:none; }
    </style>
    """
    meta = f"""
    <p><strong>Pārlapotas lapas:</strong> {pages_seen} &nbsp;|&nbsp;
       <strong>Rindas skenētas kopā:</strong> {scanned}</p>
    """
    return f"""<!doctype html><meta charset="utf-8"><title>{title}</title>{css}
    <h1>{title}</h1>
    <p><small>{datetime.now().strftime('%Y-%m-%d %H:%M')}</small></p>
    {meta}
    {('<p><em>'+extra_note+'</em></p>' if extra_note else '')}
    {body_html}
    """
# ============================================================


# ===================== MAIN =====================
async def main():
    async with async_playwright() as p:
        browser = await p.chromium.launch()
        ctx = await browser.new_context()
        page = await ctx.new_page()

        all_parsed = []
        total_scanned = 0
        pages_fetched = 0
        consecutive_empty = 0

        for n in range(1, PAGES_TOTAL + 1):
            url = LIST_URL.format(page=n)
            try:
                # domcontentloaded + a short wait for rows to render
                await page.goto(url, wait_until="domcontentloaded", timeout=180000)
                try:
                    # Wait for either rows or a small timeout (some pages render very fast)
                    await page.wait_for_selector(".flextable__row", timeout=2500)
                except TimeoutError:
                    pass
            except TimeoutError:
                # Retry once if navigation timed out
                await page.goto(url, wait_until="domcontentloaded", timeout=180000)

            # Accept cookies if shown
            try:
                for t in ["Apstiprināt", "Apstiprināt izvēlētās", "Apstiprināt visas", "Piekrītu"]:
                    btn = page.get_by_text(t, exact=False).first
                    if await btn.count() > 0:
                        await btn.click(timeout=1200)
                        break
            except:
                pass

            await page.wait_for_timeout(200)
            html = await page.content()

            if n <= 2:
                (DEBUG_DIR / f"page-{n}.html").write_text(html, encoding="utf-8")

            total_rows, parsed_rows, _ = parse_page(html)
            pages_fetched += 1
            total_scanned += total_rows

            if total_rows == 0:
                consecutive_empty += 1
                # Save the first few empties for diagnostics
                if consecutive_empty <= EMPTY_PAGE_TOLERANCE:
                    (DEBUG_DIR / f"page-{n}-EMPTY.html").write_text(html, encoding="utf-8")
                if consecutive_empty >= EMPTY_PAGE_TOLERANCE:
                    break
            else:
                consecutive_empty = 0
                all_parsed.extend(parsed_rows)

            if PAGE_DELAY_MS > 0:
                await page.wait_for_timeout(PAGE_DELAY_MS)

        await browser.close()

    # Apply filters on parsed rows
    filtered_map = {}
    for r in all_parsed:
        if filter_row(r):
            # canonicalize authority & normalized values for output
            auth_n = norm(r.get("authority"))
            r["authority"] = AUTHORITIES_NORM.get(auth_n, r.get("authority"))
            r["phase"] = norm(r.get("phase"))
            r["construction_type"] = norm(r.get("construction_type"))
            filtered_map[r["_key"]] = r  # de-dup by key
    filtered = list(filtered_map.values())

    # Delta vs previous state
    prev_state = load_state()
    delta_rows, updated_state, baseline = compute_delta(prev_state, filtered)
    save_state(updated_state)

    # Build BOTH reports (delta + full)
    delta_html = html_table_from_rows(delta_rows, include_change_col=True)
    full_html  = html_table_from_rows(filtered,   include_change_col=False)

    title_delta = "BIS – izmaiņu atskaite (jauni + stadijas izmaiņas)"
    note = "Šis ir bāzes skrējiens. Izmaiņu saraksts tiks sūtīts no nākamā skrējiena." if baseline else ""
    delta_doc = wrap_html(title_delta, delta_html, pages_fetched, total_scanned, extra_note=note)
    (REPORTS / "report_delta.html").write_text(delta_doc, encoding="utf-8")

    title_full = "BIS – pilns momentuzņēmums (atlasītie ieraksti)"
    full_doc = wrap_html(title_full, full_html, pages_fetched, total_scanned)
    (REPORTS / "report_full.html").write_text(full_doc, encoding="utf-8")

    # Useful log line
    print({
        "pages": pages_fetched,
        "rows_scanned": total_scanned,
        "rows_matched_full": len(filtered),
        "rows_reported_delta": len(delta_rows),
        "baseline": baseline,
        "empty_pages_tolerated": EMPTY_PAGE_TOLERANCE
    })

if __name__ == "__main__":
    import asyncio
    asyncio.run(main())
