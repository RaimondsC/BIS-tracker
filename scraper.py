import os, pathlib, hashlib, re, json, collections, random, time
from datetime import datetime, timedelta
from bs4 import BeautifulSoup
from playwright.async_api import async_playwright, TimeoutError
import pandas as pd

# ===================== CONFIG =====================
BASE = "https://bis.gov.lv"
# Match UI sort (Lietas numurs dilstoši)
LIST_URL = BASE + "/bisp/lv/planned_constructions/list?order=case_number&direction=desc&page={page}"

# How many list pages to attempt this run (upper bound while building baseline)
PAGES_PER_RUN = int(os.getenv("PAGES_PER_RUN", "3000"))
# How many pages to scan once baseline is complete (delta sweeps)
DELTA_SCAN_PAGES = int(os.getenv("DELTA_SCAN_PAGES", "3000"))

# Politeness / robustness
PAGE_DELAY_MS = int(os.getenv("PAGE_DELAY_MS", "200"))
MAX_RETRIES_PER_PAGE = int(os.getenv("MAX_RETRIES_PER_PAGE", "2"))
RETRY_BASE_MS = int(os.getenv("RETRY_BASE_MS", "3000"))
EMPTY_PAGE_TOLERANCE = int(os.getenv("EMPTY_PAGE_TOLERANCE", "2"))
GLOBAL_MINUTES_BUDGET = int(os.getenv("GLOBAL_MINUTES_BUDGET", "75"))
CONTEXT_ROTATE_EVERY = int(os.getenv("CONTEXT_ROTATE_EVERY", "350"))

# Folders/files
ROOT = pathlib.Path(".")
DEBUG_DIR = ROOT / "debug"; DEBUG_DIR.mkdir(parents=True, exist_ok=True)
REPORTS = ROOT / "reports"; REPORTS.mkdir(parents=True, exist_ok=True)
STATE_DIR = ROOT / "state"; STATE_DIR.mkdir(parents=True, exist_ok=True)
STATE_FILE = STATE_DIR / "state.json"              # rows you've seen (for delta)
CURSOR_FILE = STATE_DIR / "cursor.json"            # where to continue (for baseline building)
RUN_STATUS = REPORTS / "run_status.json"           # machine-readable status for workflow
BASELINE_FLAG = REPORTS / "baseline_complete.flag" # "yes" or "no" for workflow

# === Your business filters ===
AUTHORITIES_WHITELIST = {
    "RĪGAS VALSTSPILSĒTAS PAŠVALDĪBAS PILSĒTAS ATTĪSTĪBAS DEPARTAMENTS",
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

HEADER_MAP = {
    "Būvniecības kontroles institūcija": "authority",
    "Lietas numurs": "bis_number",
    "Būves nosaukums": "object",
    "Adrese": "address",
    "Būvniecības veids": "construction_type",
    "Būvniecības lietas stadija": "phase",
}

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

# ===================== UTIL =====================
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/129.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 13_6) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.4 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0 Safari/537.36",
]

def looks_like_backend_error(html: str) -> bool:
    t = (html or "").lower()
    if "503 service temporarily unavailable" in t:
        return True
    if "sistēmas kļūda" in t or "sistemas kluda" in t or "sistemu kluda" in t:
        return True
    return False

# ===================== PARSING =====================
def extract_value(cell, header_text: str) -> str:
    val_el = cell.select_one(".flextable__value")
    t = norm(val_el.get_text(" ", strip=True) if val_el else "")
    prefix = header_text + ":"
    if t.startswith(prefix):
        t = norm(t[len(prefix):])
    return t

def parse_page(html: str):
    soup = BeautifulSoup(html, "lxml")
    row_nodes = soup.select(".flextable__row")
    total_rows = len(row_nodes)
    out = []
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
        rec["_key"] = rec.get("bis_number") or hashlib.sha256(
            "|".join([rec.get("authority",""), rec.get("address",""), rec.get("object","")]).encode("utf-8")
        ).hexdigest()[:24]
        out.append(rec)
    return total_rows, out

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

def load_cursor():
    if CURSOR_FILE.exists():
        return json.loads(CURSOR_FILE.read_text(encoding="utf-8"))
    return {"next_page": 1, "baseline_complete": False}

def save_cursor(cursor):
    CURSOR_FILE.write_text(json.dumps(cursor, ensure_ascii=False, indent=2), encoding="utf-8")

def compute_delta(prev_state: dict, filtered_rows: list[dict]):
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
            updated[key] = {**old, **current, "first_seen": old.get("first_seen", today)}
    return delta, updated, baseline

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
                 "Būvniecības lietas stadija","Būvniecības veids"]]
    else:
        df = df[["BIS lieta","Būvniecības kontroles institūcija","Adrese","Būves nosaukums",
                 "Būvniecības lietas stadija","Būvniecības veids"]]
    df.rename(columns=rename, inplace=True, errors="ignore")
    return df.to_html(index=False, escape=False)

def wrap_html(title: str, body_html: str, pages_seen: int, scanned: int, notes: list[str] = None) -> str:
    css = """
    <style>
      body{font-family:system-ui,-apple-system,Segoe UI,Roboto,Arial,sans-serif; padding:16px;}
      table{border-collapse:collapse; width:100%; font-size:14px;}
      th,td{border:1px solid #e5e7eb; padding:8px; vertical-align:top;}
      th{background:#f3f4f6; text-align:left;}
      a{ text-decoration:none; }
      .note{background:#fff7ed; border:1px solid #fed7aa; padding:8px 12px; margin:12px 0;}
      .muted{color:#6b7280;}
    </style>
    """
    meta = f"""
    <p class="muted"><strong>Pārlapotas lapas:</strong> {pages_seen} &nbsp;|&nbsp;
       <strong>Rindas skenētas kopā:</strong> {scanned}</p>
    """
    notes_html = "".join(f'<div class="note">{n}</div>' for n in (notes or []))
    return f"""<!doctype html><meta charset="utf-8"><title>{title}</title>{css}
    <h1>{title}</h1>
    <p><small>{datetime.now().strftime('%Y-%m-%d %H:%M')}</small></p>
    {meta}
    {notes_html}
    {body_html}
    """

# ===================== MAIN =====================
async def main():
    start = datetime.utcnow()
    deadline = start + timedelta(minutes=GLOBAL_MINUTES_BUDGET)

    async with async_playwright() as p:
        browser = await p.chromium.launch()

        async def new_context():
            ua = random.choice(USER_AGENTS)
            ctx = await browser.new_context(
                user_agent=ua,
                locale="lv-LV",
                extra_http_headers={
                    "Accept-Language": "lv-LV,lv;q=0.9,en;q=0.8",
                    "Cache-Control": "no-cache",
                },
                viewport={"width": 1280, "height": 900}
            )
            page = await ctx.new_page()
            return ctx, page

        ctx, page = await new_context()

        cursor = load_cursor()
        baseline_complete = bool(cursor.get("baseline_complete", False))

        # Determine the page window for this run
        if baseline_complete:
            start_page = 1
            pages_to_scan = DELTA_SCAN_PAGES
        else:
            start_page = int(cursor.get("next_page", 1))
            pages_to_scan = PAGES_PER_RUN

        end_page_goal = start_page + pages_to_scan - 1

        all_parsed = []
        total_scanned = 0
        pages_fetched = 0
        consec_empty = 0
        error_pages = []
        empty_pages = []

        async def rotate_context():
            nonlocal ctx, page
            try:
                await page.close(); await ctx.close()
            except:
                pass
            ctx, page = await new_context()

        n = start_page
        while n <= end_page_goal and datetime.utcnow() < deadline:
            url = LIST_URL.format(page=n)
            retries = 0
            page_ok = False

            while retries <= MAX_RETRIES_PER_PAGE and datetime.utcnow() < deadline:
                try:
                    await page.goto(url, wait_until="domcontentloaded", timeout=180000)
                    try:
                        await page.wait_for_selector(".flextable__row", timeout=2500)
                    except TimeoutError:
                        pass

                    # accept cookies if shown
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

                    if looks_like_backend_error(html):
                        (DEBUG_DIR / f"page-{n}-ERROR-r{retries}.html").write_text(html, encoding="utf-8")
                        retries += 1
                        if retries > MAX_RETRIES_PER_PAGE:
                            error_pages.append(n)
                            break
                        backoff = RETRY_BASE_MS * (2 ** (retries - 1)) + random.randint(0, 400)
                        await page.wait_for_timeout(backoff)
                        if retries == 2:
                            await rotate_context()
                        continue

                    total_rows, parsed_rows = parse_page(html)
                    pages_fetched += 1
                    total_scanned += total_rows

                    if total_rows == 0:
                        empty_pages.append(n)
                        consec_empty += 1
                        (DEBUG_DIR / f"page-{n}-EMPTY.html").write_text(html, encoding="utf-8")
                        if consec_empty >= EMPTY_PAGE_TOLERANCE:
                            page_ok = True
                            # If we were building baseline and hit true end-of-list, mark complete.
                            if not baseline_complete:
                                baseline_complete = True
                            break
                    else:
                        consec_empty = 0
                        all_parsed.extend(parsed_rows)
                        page_ok = True
                    break

                except TimeoutError:
                    retries += 1
                    if retries > MAX_RETRIES_PER_PAGE:
                        error_pages.append(n)
                        break
                    backoff = RETRY_BASE_MS * (2 ** (retries - 1)) + random.randint(0, 400)
                    await page.wait_for_timeout(backoff)
                    if retries == 2:
                        await rotate_context()

            if PAGE_DELAY_MS > 0:
                await page.wait_for_timeout(PAGE_DELAY_MS)

            if not page_ok and not baseline_complete:
                # couldn't load this page; still move forward so we eventually cover everything
                pass

            if consec_empty >= EMPTY_PAGE_TOLERANCE:
                break

            n += 1

        # Update cursor
        next_page = n if consec_empty < EMPTY_PAGE_TOLERANCE else 1
        save_cursor({"next_page": next_page, "baseline_complete": baseline_complete})

        try:
            await page.close(); await ctx.close()
        except:
            pass
        await browser.close()

    # Apply filters on parsed rows
    filtered_map = {}
    for r in all_parsed:
        if filter_row(r):
            auth_n = norm(r.get("authority"))
            r["authority"] = AUTHORITIES_NORM.get(auth_n, r.get("authority"))
            r["phase"] = norm(r.get("phase"))
            r["construction_type"] = norm(r.get("construction_type"))
            filtered_map[r["_key"]] = r
    filtered = list(filtered_map.values())

    # Delta vs previous state
    prev_state = load_state()
    delta_rows, updated_state, baseline = compute_delta(prev_state, filtered)
    save_state(updated_state)

    # Notes
    notes = []
    if baseline or not baseline_complete:
        notes.append("Bāzes momentuzņēmums tiek veidots vairākos skrējienos (kursors turpinās nākamajā reizē).")
    if baseline_complete and not baseline:
        notes.append(f"Baseline pabeigts iepriekš. Šajā skrējienā skenētas pirmās {DELTA_SCAN_PAGES} lapas (delta).")
    if error_pages:
        notes.append(f"Servera kļūdas lapās: {', '.join(map(str, error_pages[:20]))}"
                     + (" ..." if len(error_pages) > 20 else ""))
    if empty_pages:
        notes.append(f"Tika konstatētas {len(empty_pages)} tukšas lapas. Pēc {EMPTY_PAGE_TOLERANCE} pēc kārtas meklēšana tiek pārtraukta.")

    # Build BOTH reports (we’ll choose what to attach in the workflow)
    delta_html = html_table_from_rows(delta_rows, include_change_col=True)
    full_html  = html_table_from_rows(filtered,   include_change_col=False)

    title_delta = "BIS – izmaiņu atskaite (jauni + stadijas izmaiņas)"
    delta_doc = wrap_html(title_delta, delta_html, pages_fetched, total_scanned, notes=notes if (delta_rows or baseline or error_pages) else None)
    (REPORTS / "report_delta.html").write_text(delta_doc, encoding="utf-8")

    title_full = "BIS – pilns momentuzņēmums (atlasītie ieraksti no šī skrējiena)"
    full_doc = wrap_html(title_full, full_html, pages_fetched, total_scanned, notes=notes if baseline or not baseline_complete else None)
    (REPORTS / "report_full.html").write_text(full_doc, encoding="utf-8")

    # Expose simple status for the workflow step
    status = {
        "baseline_run": baseline,
        "baseline_complete": baseline_complete,
        "pages_scanned_this_run": pages_fetched,
        "start_page": start_page,
        "end_page_goal": end_page_goal,
        "next_page": next_page
    }
    RUN_STATUS.write_text(json.dumps(status, ensure_ascii=False, indent=2), encoding="utf-8")
    BASELINE_FLAG.write_text("yes" if baseline_complete else "no", encoding="utf-8")

    print({
        "baseline_run": baseline,
        "baseline_complete": baseline_complete,
        "pages_scanned": pages_fetched,
        "rows_scanned": total_scanned,
        "rows_matched_full": len(filtered),
        "rows_reported_delta": len(delta_rows),
        "next_page": next_page
    })

if __name__ == "__main__":
    import asyncio
    asyncio.run(main())
