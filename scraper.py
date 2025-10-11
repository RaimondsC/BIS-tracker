import os, pathlib, hashlib, re, json, collections
from datetime import datetime
from bs4 import BeautifulSoup
from playwright.async_api import async_playwright, TimeoutError
import pandas as pd

# ===================== CONFIG =====================
BASE = "https://bis.gov.lv"
LIST_URL = BASE + "/bisp/lv/planned_constructions/list?page={page}"

# How many list pages to scan this run (override in the workflow's env)
PAGES_TOTAL = int(os.getenv("PAGES_TOTAL", "5000"))

# Optional politeness delay between pages (milliseconds). 0 = no delay.
PAGE_DELAY_MS = int(os.getenv("PAGE_DELAY_MS", "25"))

# Folders
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

# List header labels as emitted on /list?page=N
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
    'parsed_rows' ARE NOT filtered yet.
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

        # Every row must have a key; prefer BIS number
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
    - baseline_flag: True if this is the very first run (no previous state); we suppress email content then
    """
    baseline = (len(prev_state) == 0)
    today = datetime.now().strftime("%Y-%m-%d")

    updated = dict(prev_state)  # shallow copy
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
            # brand new record
            if not baseline:  # don't spam on the very first run
                tag = "Jauns"
                delta.append({**current, "_key": key, "_change": tag})
            updated[key] = {**current, "first_seen": today}
        else:
            # seen before: check phase change
            if old.get("phase") != phase_n:
                tag = f"Stadija: {old.get('phase','?')} → {phase_n or '?'}"
                delta.append({**current, "_key": key, "_change": tag})
            # update stored snapshot
            updated[key] = {
                **old,
                **current,
                "first_seen": old.get("first_seen", today)
            }

    return delta, updated, baseline
# =========================================================


# ===================== REPORT (HTML only) =====================
def make_html_report(rows: list[dict], pages_seen: int, scanned: int, baseline: bool) -> str:
    if baseline:
        intro = "<p><em>Šis ir bāzes skrējiens.</em> Nākamajā reizē tiks rādīti tikai jauni ieraksti un ieraksti ar stadijas izmaiņām.</p>"
    else:
        intro = ""

    if not rows:
        body = "<p>Nav jaunu vai mainītu ierakstu.</p>"
    else:
        df = pd.DataFrame(rows)[[
            "bis_number", "authority", "address", "object", "phase", "construction_type", "details_url", "_change"
        ]].copy()

        df["BIS lieta"] = df.apply(
            lambda r: (f'<a href="{r["details_url"]}" target="_blank" rel="noopener">{r["bis_number"]}</a>'
                       if r.get("details_url") and r.get("bis_number") else (r.get("bis_number",""))),
            axis=1
        )
        df.rename(columns={
            "_change": "Izmaiņas",
            "authority": "Būvniecības kontroles institūcija",
            "address": "Adrese",
            "object": "Būves nosaukums",
            "phase": "Būvniecības lietas stadija",
            "construction_type": "Būvniecības veids",
        }, inplace=True)
        df = df[["Izmaiņas","BIS lieta","Būvniecības kontroles institūcija","Adrese","Būves nosaukums",
                 "Būvniecības lietas stadija","Būvniecības veids"]]
        body = df.to_html(index=False, escape=False)

    meta = f"""
    <p><strong>Pārlapotas lapas:</strong> {pages_seen} &nbsp;|&nbsp;
       <strong>Rindas skenētas kopā:</strong> {scanned} &nbsp;|&nbsp;
       <strong>Ziņojamo ierakstu skaits:</strong> {len(rows)}</p>
    """
    css = """
    <style>
      body{font-family:system-ui,-apple-system,Segoe UI,Roboto,Arial,sans-serif; padding:16px;}
      table{border-collapse:collapse; width:100%; font-size:14px;}
      th,td{border:1px solid #e5e7eb; padding:8px; vertical-align:top;}
      th{background:#f3f4f6; text-align:left;}
      a{ text-decoration:none; }
    </style>
    """
    return f"""<!doctype html><meta charset="utf-8"><title>BIS atskaite</title>{css}
    <h1>BIS plānoto būvniecību izmaiņu atskaite</h1>
    <p><small>{datetime.now().strftime('%Y-%m-%d %H:%M')}</small></p>
    {meta}
    {intro}
    {body}
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
        diag_all = collections.Counter()

        for n in range(1, PAGES_TOTAL + 1):
            url = LIST_URL.format(page=n)
            try:
                await page.goto(url, wait_until="domcontentloaded", timeout=180000)
            except TimeoutError:
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

            total_rows, parsed_rows, diag = parse_page(html)
            pages_fetched += 1
            total_scanned += total_rows
            diag_all.update(diag)

            if total_rows == 0:  # true end-of-list
                break

            all_parsed.extend(parsed_rows)

            if PAGE_DELAY_MS > 0:
                await page.wait_for_timeout(PAGE_DELAY_MS)

        await browser.close()

    # Apply filters on parsed rows
    filtered = []
    for r in all_parsed:
        if filter_row(r):
            # canonicalize authority & normalized values for output
            auth_n = norm(r.get("authority"))
            r["authority"] = AUTHORITIES_NORM.get(auth_n, r.get("authority"))
            r["phase"] = norm(r.get("phase"))
            r["construction_type"] = norm(r.get("construction_type"))
            filtered.append(r)

    # Delta vs previous state
    prev_state = load_state()
    delta_rows, updated_state, baseline = compute_delta(prev_state, filtered)
    save_state(updated_state)

    # HTML report (only delta)
    html = make_html_report(delta_rows, pages_fetched, total_scanned, baseline)
    (REPORTS / "report.html").write_text(html, encoding="utf-8")

    # Useful log line
    print({
        "pages": pages_fetched,
        "rows_scanned": total_scanned,
        "rows_matched_now": len(filtered),
        "rows_reported_delta": len(delta_rows),
        "baseline": baseline
    })

if __name__ == "__main__":
    import asyncio
    asyncio.run(main())
