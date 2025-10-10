import os, re, pathlib, hashlib
from datetime import datetime
from bs4 import BeautifulSoup
from playwright.async_api import async_playwright, TimeoutError
import pandas as pd

BASE = "https://bis.gov.lv"
LIST_URL = BASE + "/bisp/lv/planned_constructions/list?page={page}"

# ---------------- Your filters (exact strings) ----------------
AUTHORITIES_WHITELIST = {
    "RĪGAS VALSTSPILSĪTAS PAŠVALDĪBAS PILSĒTAS ATTĪSTĪBAS DEPARTAMENTS".replace("ĪTAS","TAS"),
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
# ----------------------------------------------------------------

# Max pages to fetch (1-based). Set PAGES_TOTAL=300 in workflow env.
PAGES_TOTAL = int(os.getenv("PAGES_TOTAL", "300"))

DEBUG_DIR = pathlib.Path("debug"); DEBUG_DIR.mkdir(exist_ok=True)

def stable_row_id(r: dict) -> str:
    if r.get("bis_number"):
        return f"bis:{r['bis_number']}"
    key = "|".join(str(r.get(k, "")) for k in ["authority","address","object","phase","construction_type"])
    return "h:" + hashlib.sha256(key.encode("utf-8")).hexdigest()[:16]

def parse_table(html: str) -> list[dict]:
    soup = BeautifulSoup(html, "lxml")
    table = soup.find("table") or soup.find(attrs={"role": "table"})
    if not table:
        return []

    headers = [th.get_text(" ", strip=True) for th in table.find_all("th")]

    def val(cells, names):
        for n in names:
            if n in headers:
                return cells[headers.index(n)]
        return ""

    out = []
    for tr in table.find_all("tr"):
        tds = tr.find_all("td")
        if not tds:
            continue
        cells = [td.get_text(" ", strip=True) for td in tds]
        a = tr.find("a", href=True)
        link = None
        if a and "/bisp" in a["href"]:
            link = a["href"]
            if link.startswith("/"):
                link = BASE + link

        rec = {
            "bis_number":        val(cells, ["Lietas numurs","BIS lietas numurs","Lietas Nr."]),
            "authority":         val(cells, ["Būvniecības kontroles institūcija","Institūcija","Būvvalde"]),
            "address":           val(cells, ["Adrese","Būvobjekta adrese"]),
            "object":            val(cells, ["Būvobjekts","Nosaukums","Objekts"]),
            "phase":             val(cells, ["Būvniecības lietas stadija","Stadija","Statuss"]),
            "construction_type": val(cells, ["Būvniecības veids","Veids"]),
            "details_url":       link,
        }

        # Local filters
        if rec["authority"] not in AUTHORITIES_WHITELIST:
            continue
        if rec["phase"] and rec["phase"] not in PHASE_KEEP:
            continue
        if rec["construction_type"] and rec["construction_type"] not in TYPE_KEEP:
            continue

        rec["id"] = stable_row_id(rec)
        out.append(rec)
    return out

def save_reports(rows: list[dict], pages_seen: int):
    reports = pathlib.Path("reports")
    reports.mkdir(parents=True, exist_ok=True)

    # de-dup by id
    df = pd.DataFrame(rows)
    if not df.empty:
        df.drop_duplicates(subset=["id"], inplace=True)

    df.to_csv(reports / "latest.csv", index=False)
    today = datetime.now().strftime("%Y-%m-%d")
    df.to_csv(reports / f"{today}.csv", index=False)

    (reports / "CHANGELOG.md").write_text(
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
                # try once more quickly
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

            # tiny pause to let table render
            await page.wait_for_timeout(400)

            html = await page.content()
            if n <= 2:
                (DEBUG_DIR / f"page-{n}.html").write_text(html, encoding="utf-8")

            rows = parse_table(html)

            if not rows:
                empty_streak += 1
                # If we hit 2 empty pages in a row, assume end of listing
                if empty_streak >= 2:
                    break
            else:
                empty_streak = 0
                all_rows.extend(rows)

        await browser.close()

    save_reports(all_rows, n)

if __name__ == "__main__":
    import asyncio
    asyncio.run(main())
