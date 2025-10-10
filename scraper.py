import os, pathlib, hashlib
from datetime import datetime
from bs4 import BeautifulSoup
from playwright.async_api import async_playwright, TimeoutError
import pandas as pd

BASE = "https://bis.gov.lv"
LIST_URL = BASE + "/bisp/lv/planned_constructions/list?page={page}"

# ---------------- Your filters (exact strings) ----------------
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
# --------------------------------------------------------------

# Pages to fetch (1..N). Set PAGES_TOTAL in workflow env; default 300
PAGES_TOTAL = int(os.getenv("PAGES_TOTAL", "300"))

DEBUG_DIR = pathlib.Path("debug"); DEBUG_DIR.mkdir(exist_ok=True)
REPORTS = pathlib.Path("reports"); REPORTS.mkdir(parents=True, exist_ok=True)

# Map the LV header names we saw in the HTML (div-based "flextable")
# page-1.html / page-2.html confirm these exact labels:
# - "Būvniecības kontroles institūcija"
# - "Lietas numurs"
# - "Būves nosaukums"
# - "Adrese"
# - "Būvniecības veids"
# - "Būvniecības lietas stadija"
HEADER_MAP = {
    "Būvniecības kontroles institūcija": "authority",
    "Lietas numurs": "bis_number",
    "Būves nosaukums": "object",
    "Adrese": "address",
    "Būvniecības veids": "construction_type",
    "Būvniecības lietas stadija": "phase",
}

def stable_row_id(r: dict) -> str:
    if r.get("bis_number"):
        return f"bis:{r['bis_number']}"
    key = "|".join(str(r.get(k, "")) for k in ["authority","address","object","phase","construction_type"])
    return "h:" + hashlib.sha256(key.encode("utf-8")).hexdigest()[:16]

def parse_flextable(html: str) -> list[dict]:
    soup = BeautifulSoup(html, "lxml")
    rows = soup.select(".flextable__row")
    out = []
    for row in rows:
        rec = {"details_url": None}
        # each cell has data-column-header-name and a .flextable__value
        for cell in row.select(".flextable__cell"):
            header = (cell.get("data-column-header-name") or "").strip()
            key = HEADER_MAP.get(header)
            if not key:
                continue
            val_el = cell.select_one(".flextable__value")
            text = (val_el.get_text(" ", strip=True) if val_el else "").strip()
            # BIS number also carries a link
            a = cell.select_one("a.public_list__link[href]")
            if key == "bis_number" and a:
                href = a.get("href", "")
                if href.startswith("/"):
                    href = BASE + href
                rec["details_url"] = href
                # prefer the link text for number
                text = a.get_text(" ", strip=True)
            rec[key] = text

        # Local filters
        if not rec.get("authority") or rec["authority"] not in AUTHORITIES_WHITELIST:
            continue
        if rec.get("phase") and rec["phase"] not in PHASE_KEEP:
            continue
        if rec.get("construction_type") and rec["construction_type"] not in TYPE_KEEP:
            continue

        rec["id"] = stable_row_id(rec)
        out.append(rec)
    return out

def save_reports(rows: list[dict], pages_seen: int):
    df = pd.DataFrame(rows)
    if not df.empty:
        df.drop_duplicates(subset=["id"], inplace=True)
    df.to_csv(REPORTS / "latest.csv", index=False)
    today = datetime.now().strftime("%Y-%m-%d")
    df.to_csv(REPORTS / f"{today}.csv", index=False)
    (REPORTS / "CHANGELOG.md").write_text(
        "# Snapshot {}\n\n- Pārlapotas lapas: {}\n- Rindas pēc filtriem (unikālas): {}\n"
        .format(datetime.now().strftime("%Y-%m-%d %H:%M"), pages_seen, 0 if df.empty else len(df)),
        encoding="utf-8"
    )

async def main():
    async with async_playwright() as p:
        browser = await p.chromium.launch()
        ctx = await browser.new_context()
        page = await ctx.new_page()

        all_rows = []
        empty_streak = 0

        for n in range(1, PAGES_TOTAL + 1):
            url = LIST_URL.format(page=n)
            try:
                await page.goto(url, wait_until="domcontentloaded", timeout=180000)
            except TimeoutError:
                await page.goto(url, wait_until="domcontentloaded", timeout=180000)

            # Accept cookies once if shown
            try:
                for t in ["Apstiprināt", "Apstiprināt visas", "Piekrītu"]:
                    btn = page.get_by_text(t, exact=False).first
                    if await btn.count() > 0:
                        await btn.click(timeout=1200)
                        break
            except:
                pass

            await page.wait_for_timeout(250)  # let the list render a moment
            html = await page.content()

            # keep first two pages for debugging
            if n <= 2:
                (DEBUG_DIR / f"page-{n}.html").write_text(html, encoding="utf-8")

            # parse the DIV-based "flextable"
            rows = parse_flextable(html)

            if not rows:
                empty_streak += 1
                if empty_streak >= 2:
                    # assume end-of-results
                    break
            else:
                empty_streak = 0
                all_rows.extend(rows)

        await browser.close()

    save_reports(all_rows, n)

if __name__ == "__main__":
    import asyncio
    asyncio.run(main())
