import os, pathlib, hashlib, re, json, collections, random, time
from datetime import datetime, timedelta
from bs4 import BeautifulSoup
from playwright.async_api import async_playwright, TimeoutError
import pandas as pd

# ===================== CONFIG =====================
BASE = "https://bis.gov.lv"
LIST_URL = BASE + "/bisp/lv/planned_constructions/list?order=case_number&direction=desc&page={page}"

# Build baseline up to ceiling, then delta
TARGET_MAX_PAGE   = int(os.getenv("TARGET_MAX_PAGE", "3000"))   # stop baseline at this page, then wrap to 1
PAGES_PER_RUN     = int(os.getenv("PAGES_PER_RUN", "2500"))     # advance cursor per baseline run
DELTA_SCAN_PAGES  = int(os.getenv("DELTA_SCAN_PAGES", "3000"))  # pages to scan per run after baseline complete
FRONT_REFRESH_PAGES = int(os.getenv("FRONT_REFRESH_PAGES", "0"))  # e.g., "20" to lightly refresh very front pages

# Politeness / robustness
PAGE_DELAY_MS        = int(os.getenv("PAGE_DELAY_MS", "400"))
MAX_RETRIES_PER_PAGE = int(os.getenv("MAX_RETRIES_PER_PAGE", "0"))   # retries beyond 0 rarely help here
RETRY_BASE_MS        = int(os.getenv("RETRY_BASE_MS", "2000"))
GLOBAL_MINUTES_BUDGET = int(os.getenv("GLOBAL_MINUTES_BUDGET", "75"))
CONTEXT_ROTATE_EVERY = int(os.getenv("CONTEXT_ROTATE_EVERY", "350"))

# Error-storm handling
ERROR_BAIL_WINDOW     = int(os.getenv("ERROR_BAIL_WINDOW", "30"))   # pages to consider
ERROR_BAIL_THRESHOLD  = float(os.getenv("ERROR_BAIL_THRESHOLD", "0.80"))  # >=80% errors -> storm
COOLDOWN_ON_STORM_MINUTES = int(os.getenv("COOLDOWN_ON_STORM_MINUTES", "9"))
STORM_COOLDOWNS_MAX   = int(os.getenv("STORM_COOLDOWNS_MAX", "2"))

# Failed-pages-first queue
FAILED_PAGE_RETRY_LIMIT  = int(os.getenv("FAILED_PAGE_RETRY_LIMIT", "400"))  # how many to try first each run
FAILED_PAGE_MAX_ATTEMPTS = int(os.getenv("FAILED_PAGE_MAX_ATTEMPTS", "8"))   # then drop from queue

# Files/dirs
ROOT = pathlib.Path(".")
DEBUG_DIR = ROOT / "debug"; DEBUG_DIR.mkdir(parents=True, exist_ok=True)
REPORTS = ROOT / "reports"; REPORTS.mkdir(parents=True, exist_ok=True)
STATE_DIR = ROOT / "state"; STATE_DIR.mkdir(parents=True, exist_ok=True)
STATE_FILE = STATE_DIR / "state.json"               # seen rows (for delta)
CURSOR_FILE = STATE_DIR / "cursor.json"             # next_page + baseline_complete
FAILED_FILE = STATE_DIR / "failed_pages.json"       # pages to retry first (with attempts)
RUN_STATUS  = REPORTS / "run_status.json"
BASELINE_FLAG = REPORTS / "baseline_complete.flag"  # "yes" / "no"

# === Filters ===
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

def load_json(path: pathlib.Path, default):
    try:
        if path.exists():
            return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        pass
    return default

def dump_json(path: pathlib.Path, obj):
    path.write_text(json.dumps(obj, ensure_ascii=False, indent=2), encoding="utf-8")

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
    return load_json(STATE_FILE, {})

def save_state(state: dict):
    dump_json(STATE_FILE, state)

def load_cursor():
    return load_json(CURSOR_FILE, {"next_page": 1, "baseline_complete": False})

def save_cursor(c):
    dump_json(CURSOR_FILE, c)

def load_failed_queue():
    data = load_json(FAILED_FILE, {"pages": []})
    cleaned, seen = [], set()
    for item in data.get("pages", []):
        try:
            n = int(item.get("n"))
            att = int(item.get("attempts", 0))
            if n not in seen:
                cleaned.append({"n": n, "attempts": max(0, att)})
                seen.add(n)
        except Exception:
            continue
    return {"pages": cleaned}

def save_failed_queue(queue):
    kept = [p for p in queue.get("pages", []) if p.get("attempts", 0) < FAILED_PAGE_MAX_ATTEMPTS]
    dump_json(FAILED_FILE, {"pages": kept})

def push_failed_page(queue, n):
    if n > TARGET_MAX_PAGE:
        return
    for item in queue["pages"]:
        if item["n"] == n:
            item["attempts"] = min(item.get("attempts", 0) + 1, FAILED_PAGE_MAX_ATTEMPTS)
            return
    queue["pages"].append({"n": n, "attempts": 1})

def pop_failed_batch(queue, limit):
    batch, rest = [], []
    for item in queue["pages"]:
        if len(batch) < limit:
            if int(item.get("n", 0)) <= TARGET_MAX_PAGE:
                batch.append(item)
            else:
                rest.append(item)
        else:
            rest.append(item)
    queue["pages"] = rest
    return batch

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

# ===================== REPORTS =====================
def html_table_from_rows(rows: list[dict], include_change_col: bool) -> str:
    if not rows:
        return "<p>Nav datu.</p>"
    df = pd.DataFrame(rows)
    base_cols = ["bis_number","authority","address","object","phase","construction_type","details_url"]
    if include_change_col:
        base_cols = ["_change"] + base_cols
    for c in base_cols:
        if c not in df.columns:
            df[c] = ""
    def linkify(r):
        bn = r.get("bis_number", "")
        url = r.get("details_url", "")
        return f'<a href="{url}" target="_blank" rel="noopener">{bn}</a>' if bn and url else bn
    df["BIS lieta"] = df.apply(linkify, axis=1)
    df.rename(columns={
        "authority": "Būvniecības kontroles institūcija",
        "address": "Adrese",
        "object": "Būves nosaukums",
        "phase": "Būvniecības lietas stadija",
        "construction_type": "Būvniecības veids",
        "_change": "Izmaiņas",
    }, inplace=True)
    if include_change_col:
        wanted = ["Izmaiņas","BIS lieta","Būvniecības kontroles institūcija","Adrese",
                  "Būves nosaukums","Būvniecības lietas stadija","Būvniecības veids"]
    else:
        wanted = ["BIS lieta","Būvniecības kontroles institūcija","Adrese",
                  "Būves nosaukums","Būvniecības lietas stadija","Būvniecības veids"]
    existing = [c for c in wanted if c in df.columns]
    df = df[existing]
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
        next_page = int(cursor.get("next_page", 1))

        # Build worklist
        failed_queue = load_failed_queue()
        failed_batch = [i["n"] for i in pop_failed_batch(failed_queue, FAILED_PAGE_RETRY_LIMIT)]

        worklist, visited = [], set()
        # 1) failed pages first (within ceiling)
        for n in failed_batch:
            if 1 <= int(n) <= TARGET_MAX_PAGE and n not in visited:
                worklist.append(int(n)); visited.add(int(n))
        # 2) optional front refresh during baseline
        if not baseline_complete and FRONT_REFRESH_PAGES > 0:
            for n in range(1, min(FRONT_REFRESH_PAGES, TARGET_MAX_PAGE) + 1):
                if n not in visited:
                    worklist.append(n); visited.add(n)
        # 3) sequential window (baseline vs delta)
        if baseline_complete:
            seq_start = 1
            seq_end   = min(TARGET_MAX_PAGE, DELTA_SCAN_PAGES)
        else:
            seq_start = max(1, next_page)
            seq_end   = min(TARGET_MAX_PAGE, seq_start + PAGES_PER_RUN - 1)
        for n in range(seq_start, seq_end + 1):
            if n not in visited:
                worklist.append(n); visited.add(n)

        # Scrape
        pages_fetched = 0
        total_scanned = 0
        consec_empty = 0
        error_pages, empty_pages = [], []
        recent_flags = collections.deque(maxlen=ERROR_BAIL_WINDOW)  # 1=error, 0=ok
        seq_last_scanned = seq_start - 1
        storm_cooldowns_used = 0

        async def rotate_context():
            nonlocal ctx, page
            try:
                await page.close(); await ctx.close()
            except:
                pass
            ctx, page = await new_context()

        async def fetch_and_parse(page_no: int):
            retries = 0
            while retries <= MAX_RETRIES_PER_PAGE and datetime.utcnow() < deadline:
                try:
                    await page.goto(LIST_URL.format(page=page_no), wait_until="domcontentloaded", timeout=180000)
                    try:
                        await page.wait_for_selector(".flextable__row", timeout=2500)
                    except TimeoutError:
                        pass
                    # cookies banner
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
                        (DEBUG_DIR / f"page-{page_no}-ERROR-r{retries}.html").write_text(html, encoding="utf-8")
                        retries += 1
                        if retries > MAX_RETRIES_PER_PAGE:
                            return "error", 0, []
                        backoff = RETRY_BASE_MS * (2 ** (retries - 1)) + random.randint(0, 400)
                        await page.wait_for_timeout(backoff)
                        if retries == 1:
                            await rotate_context()
                        continue

                    total_rows, parsed_rows = parse_page(html)
                    return ("empty" if total_rows == 0 else "ok"), total_rows, parsed_rows

                except TimeoutError:
                    retries += 1
                    if retries > MAX_RETRIES_PER_PAGE:
                        return "error", 0, []
                    backoff = RETRY_BASE_MS * (2 ** (retries - 1)) + random.randint(0, 400)
                    await page.wait_for_timeout(backoff)
                    if retries == 1:
                        await rotate_context()
            return "error", 0, []

        async def maybe_cooldown(context_label: str) -> bool:
            """Return True if we cooled down (and should continue), False if we must bail."""
            nonlocal storm_cooldowns_used, recent_flags
            if len(recent_flags) == ERROR_BAIL_WINDOW and (sum(recent_flags) / len(recent_flags)) >= ERROR_BAIL_THRESHOLD:
                if storm_cooldowns_used < STORM_COOLDOWNS_MAX and datetime.utcnow() + timedelta(minutes=COOLDOWN_ON_STORM_MINUTES) < deadline:
                    storm_cooldowns_used += 1
                    (DEBUG_DIR / f"COOLDOWN_{storm_cooldowns_used}_{context_label}.txt").write_text(
                        f"Error storm detected (≥{ERROR_BAIL_THRESHOLD*100:.0f}% over {ERROR_BAIL_WINDOW} pages). "
                        f"Cooling down for {COOLDOWN_ON_STORM_MINUTES} minutes.",
                        encoding="utf-8"
                    )
                    await page.wait_for_timeout(COOLDOWN_ON_STORM_MINUTES * 60 * 1000)
                    await rotate_context()
                    recent_flags.clear()
                    return True
                else:
                    (DEBUG_DIR / "ERROR_STORM.txt").write_text(
                        f"High error rate in last {ERROR_BAIL_WINDOW} pages (≥{ERROR_BAIL_THRESHOLD*100:.0f}%). Bailing.",
                        encoding="utf-8"
                    )
                    return False
            return True

        # 1) failed pages first (do not let empties stop; they don't indicate end)
        visited_set = set()
        for n in [x for x in worklist if x in failed_batch]:
            if datetime.utcnow() >= deadline:
                break
            status, rows, parsed = await fetch_and_parse(n)
            visited_set.add(n)

            if status == "error":
                push_failed_page(failed_queue, n)
                error_pages.append(n)
                recent_flags.append(1)
            else:
                pages_fetched += 1
                total_scanned += rows
                if status == "ok":
                    all_rows = parsed
                    # accumulate
                    if 'all_parsed' in locals():
                        all_parsed.extend(all_rows)
                    else:
                        all_parsed = list(all_rows)
                recent_flags.append(0)

            if PAGE_DELAY_MS > 0:
                await page.wait_for_timeout(PAGE_DELAY_MS)

            cont = await maybe_cooldown("failed-first")
            if not cont:
                break

        # ensure accumulator exists
        if 'all_parsed' not in locals():
            all_parsed = []

        # 2) sequential window (empties count toward end-of-list during baseline)
        n = seq_start
        while n <= seq_end and datetime.utcnow() < deadline:
            if n in visited_set:
                n += 1
                continue

            status, rows, parsed = await fetch_and_parse(n)

            if status == "error":
                push_failed_page(failed_queue, n)
                error_pages.append(n)
                recent_flags.append(1)
            elif status == "empty":
                empty_pages.append(n)
                recent_flags.append(0)
                pages_fetched += 1
                if not baseline_complete:
                    consec_empty += 1
                    if consec_empty >= 2:
                        baseline_complete = True
                        break
            else:
                consec_empty = 0 if not baseline_complete else consec_empty
                pages_fetched += 1
                total_scanned += rows
                all_parsed.extend(parsed)
                recent_flags.append(0)
                seq_last_scanned = max(seq_last_scanned, n)

            if PAGE_DELAY_MS > 0:
                await page.wait_for_timeout(PAGE_DELAY_MS)

            cont = await maybe_cooldown("sequential")
            if not cont:
                break

            n += 1

        try:
            await page.close(); await ctx.close()
        except:
            pass
        await browser.close()

    # Next cursor
    if not baseline_complete:
        if seq_last_scanned >= seq_start:
            nxt = seq_last_scanned + 1
        else:
            nxt = next_page
        if nxt > TARGET_MAX_PAGE:
            baseline_complete = True
            nxt = 1
        save_cursor({"next_page": nxt, "baseline_complete": baseline_complete})
    else:
        save_cursor({"next_page": 1, "baseline_complete": True})

    save_failed_queue(failed_queue)

    # Filter & de-dup
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
    if not baseline_complete:
        notes.append(f"Bāzes momentuzņēmums līdz {TARGET_MAX_PAGE}. lpp. tiek veidots vairākos skrējienos.")
        if FRONT_REFRESH_PAGES > 0:
            notes.append(f"Katrā skrējienā papildus pārbaudītas pirmās {FRONT_REFRESH_PAGES} lapas.")
    else:
        notes.append(f"Baseline līdz {TARGET_MAX_PAGE}. lpp. pabeigts. Šajā skrējienā skenētas pirmās {min(TARGET_MAX_PAGE, DELTA_SCAN_PAGES)} lapas (delta).")
    if error_pages:
        notes.append(f"Servera kļūdas lapās: {', '.join(map(str, error_pages[:20]))}" + (" ..." if len(error_pages) > 20 else ""))
    if empty_pages:
        notes.append(f"Tika konstatētas {len(empty_pages)} tukšas lapas.")
    elapsed = int((datetime.utcnow() - start).total_seconds() // 60)
    notes.append(f"Skrējiena laiks: ~{elapsed} min.")

    # Reports
    def html_table(rows, include_change): return html_table_from_rows(rows, include_change)
    delta_html = html_table(delta_rows, include_change=True)
    full_html  = html_table(filtered, include_change=False)

    title_delta = "BIS – izmaiņu atskaite (jauni + stadijas izmaiņas)"
    delta_doc = wrap_html(title_delta, delta_html, pages_fetched, total_scanned,
                          notes=notes if (delta_rows or baseline or error_pages) else None)
    (REPORTS / "report_delta.html").write_text(delta_doc, encoding="utf-8")

    title_full = "BIS – pilns momentuzņēmums (atlasītie ieraksti no šī skrējiena)"
    full_doc = wrap_html(title_full, full_html, pages_fetched, total_scanned,
                         notes=notes if (baseline or not baseline_complete) else None)
    (REPORTS / "report_full.html").write_text(full_doc, encoding="utf-8")

    status = {
        "baseline_run": baseline,
        "baseline_complete": baseline_complete,
        "pages_scanned_this_run": pages_fetched,
        "seq_start": seq_start,
        "seq_end": seq_end,
        "seq_last_scanned": seq_last_scanned,
        "next_page": load_json(CURSOR_FILE, {}).get("next_page", 1),
        "failed_queue_size": len(load_json(FAILED_FILE, {"pages": []}).get("pages", [])),
        "storm_cooldowns_used": storm_cooldowns_used
    }
    RUN_STATUS.write_text(json.dumps(status, ensure_ascii=False, indent=2), encoding="utf-8")
    BASELINE_FLAG.write_text("yes" if baseline_complete else "no", encoding="utf-8")

    print(status)

if __name__ == "__main__":
    import asyncio
    asyncio.run(main())
