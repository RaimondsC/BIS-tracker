import os, pathlib, hashlib, re, collections
from datetime import datetime
from bs4 import BeautifulSoup
from playwright.async_api import async_playwright, TimeoutError
import pandas as pd

BASE = "https://bis.gov.lv"
LIST_URL = BASE + "/bisp/lv/planned_constructions/list?page={page}"

# ---- Filters (we normalize text before comparing)
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
# ----

PAGES_TOTAL = int(os.getenv("PAGES_TOTAL", "30"))
DEBUG_DIR = pathlib.Path("debug"); DEBUG_DIR.mkdir(exist_ok=True)
REPORTS = pathlib.Path("reports"); REPORTS.mkdir(parents=True, exist_ok=True)

HEADER_MAP = {
    "Būvniecības kontroles institūcija": "authority",
    "Lietas numurs": "bis_number",
    "Būves nosaukums": "object",
    "Adrese": "address",
    "Būvniecības veids": "construction_type",
    "Būvniecības lietas stadija": "phase",
}

NBSP = "\u00A0"
def norm(s: str) -> str:
    if s is None: return ""
    s = s.replace(NBSP, " ")
    s = re.sub(r"\s+", " ", s)
    return s.strip()

# pre-normalize filters
AUTHORITIES_NORM = {norm(a): a for a in AUTHORITIES_WHITELIST}
PHASE_KEEP_NORM = {norm(x) for x in PHASE_KEEP}
TYPE_KEEP_NORM = {norm(x) for x in TYPE_KEEP}

def stable_row_id(r: dict) -> str:
    if r.get("bis_number"):
        return f"bis:{r['bis_number']}"
    key = "|".join(str(r.get(k, "")) for k in ["authority","address","object","phase","construction_type"])
    return "h:" + hashlib.sha256(key.encode("utf-8")).hexdigest()[:16]

def extract_value(cell, header_text: str) -> str:
    """Take value text and strip the leading 'Label:' prefix injected for screen readers."""
    val_el = cell.select_one(".flextable__value")
    t = norm(val_el.get_text(" ", strip=True) if val_el else "")
    prefix = header_text + ":"
    if t.startswith(prefix):
        t = norm(t[len(prefix):])
    return t

def parse_page(html: str):
    """Return (total_rows_on_page, matched_rows_list, diag Counter by authority)."""
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

        auth_n = norm(rec.get("authority"))
        if auth_n:
            diag[auth_n] += 1

        # normalized filtering
        if auth_n not in AUTHORITIES_NORM:
            continue
        phase_n = norm(rec.get("phase"))
        if phase_n and phase_n not in PHASE_KEEP_NORM:
            continue
        type_n = norm(rec.get("construction_type"))
        if type_n and type_n not in TYPE_KEEP_NORM:
            continue

        rec["authority"] = AUTHORITIES_NORM.get(auth_n, rec.get("authority"))
        rec["phase"] = phase_n
        rec["construction_type"] = type_n
        rec["id"] = stable_row_id(rec)
        out.append(rec)

    return total_rows, out, diag

def save_reports(rows: list[dict], pages_seen: int, scanned: int, diag_accum: collections.Counter):
    df = pd.DataFrame(rows)
    if not df.empty:
        df.drop_duplicates(subset=["id"], inplace=True)
    df.to_csv(REPORTS / "latest.csv", index=False)
    today = datetime.now().strftime("%Y-%m-%d")
    df.to_csv(REPORTS / f"{today}.csv", index=False)

    top_diag = "\n".join(f"  - {auth}: {cnt}" for auth, cnt in diag_accum.most_common(10))
    (REPORTS / "CHANGELOG.md").write_text(
        "# Snapshot {}\n\n"
        "- Pārlapotas lapas: {}\n"
        "- Rindas skenētas kopā: {}\n"
        "- Rindas pēc filtriem (unikālas): {}\n"
        "- Populārākās iestādes šajā skrējienā:\n{}\n"
        .format(datetime.now().strftime("%Y-%m-%d %H:%M"),
                pages_seen, scanned, 0 if df.empty else len(df),
                top_diag or "  (nav datu)"),
        encoding="utf-8"
    )

async def main():
    async with async_playwright() as p:
        browser = await p.chromium.launch()
        ctx = await browser.new_context()
        page = await ctx.new_page()

        all_rows = []
        total_scanned = 0
        pages_fetched = 0
        diag_all = collections.Counter()

        for n in range(1, PAGES_TOTAL + 1):
            url = LIST_URL.format(page=n)
            try:
                await page.goto(url, wait_until="domcontentloaded", timeout=180000)
            except TimeoutError:
                await page.goto(url, wait_until="domcontentloaded", timeout=180000)

            # Accept cookies, if shown
            try:
                for t in ["Apstiprināt", "Apstiprināt izvēlētās", "Apstiprināt visas", "Piekrītu"]:
                    btn = page.get_by_text(t, exact=False).first
                    if await btn.count() > 0:
                        await btn.click(timeout=1200)
                        break
            except:
                pass

            await page.wait_for_timeout(250)
            html = await page.content()

            if n <= 2:
                (DEBUG_DIR / f"page-{n}.html").write_text(html, encoding="utf-8")

            total_rows, matched, diag = parse_page(html)
            pages_fetched += 1
            total_scanned += total_rows
            diag_all.update(diag)

            if total_rows == 0:  # true end-of-list
                break

            all_rows.extend(matched)

        await browser.close()

    save_reports(all_rows, pages_fetched, total_scanned, diag_all)
    print({
        "pages": pages_fetched,
        "rows_scanned": total_scanned,
        "rows_matched": len(all_rows),
        "top_authorities_seen": diag_all.most_common(5),
    })

if __name__ == "__main__":
    import asyncio
    asyncio.run(main())
