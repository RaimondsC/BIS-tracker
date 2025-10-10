import os, pathlib, hashlib
from datetime import datetime
from bs4 import BeautifulSoup
from playwright.async_api import async_playwright, TimeoutError
import pandas as pd
import re

BASE = "https://bis.gov.lv"
LIST_URL = BASE + "/bisp/lv/planned_constructions/list?page={page}"

# Your filters (exact strings)
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

# Pages to fetch (set in workflow env, default 300)
PAGES_TOTAL = int(os.getenv("PAGES_TOTAL", "300"))

DEBUG_DIR = pathlib.Path("debug"); DEBUG_DIR.mkdir(exist_ok=True)
REPORTS = pathlib.Path("reports"); REPORTS.mkdir(parents=True, exist_ok=True)

LABELS = {
    "authority": "Būvniecības kontroles institūcija",
    "bis_number": "Lietas numurs",
    "object": "Būves nosaukums",      # sometimes "Būvobjekts" elsewhere; we’ll check both
    "address": "Adrese",
    "construction_type": "Būvniecības veids",
    "phase": "Būvniecības lietas stadija",
}

def stable_row_id(r: dict) -> str:
    if r.get("bis_number"):
        return f"bis:{r['bis_number']}"
    key = "|".join(str(r.get(k, "")) for k in ["authority","address","object","phase","construction_type"])
    return "h:" + hashlib.sha256(key.encode("utf-8")).hexdigest()[:16]

def text_of(el):
    return (el.get_text(" ", strip=True) if el else "").strip()

def parse_listing_blocks(html: str) -> list[dict]:
    """
    Parse the /list?page=N view, which shows repeated "Lieta …" blocks with label/value lines.
    We detect each block by the H3 heading "Lieta …" and then read the following siblings
    until the next H3.
    """
    soup = BeautifulSoup(html, "lxml")
    # Blocks are separated by <h3> with text starting "Lieta "
    blocks = []
    for h3 in soup.find_all(re.compile("^h\\d$")):
        t = text_of(h3)
        if not t.startswith("Lieta "):
            continue
        # collect siblings until next header of the same level
        items = []
        for sib in h3.next_siblings:
            if getattr(sib, "name", None) and re.fullmatch("^h\\d$", sib.name) and text_of(sib).startswith("Lieta "):
                break
            items.append(sib)
        blocks.append((h3, items))

    out = []
    for h3, items in blocks:
        rec = {}
        # default details_url from "Lietas numurs" link (if any)
        details_url = None

        # Grab lines — many sites wrap each line in <p>, <div>, or <li>
        lines = []
        for el in items:
            if getattr(el, "name", None) in ("p","div","li"):
                lines.append(text_of(el))
                # also keep any anchors
                for a in el.find_all("a", href=True):
                    lines.append("A_HREF::" + a["href"].strip() + "||" + text_of(a))

        # Parse label:value lines
        for ln in lines:
            if ln.startswith("A_HREF::"):
                href, atext = ln.split("||", 1)[0][8:], ln.split("||", 1)[1]
                if "bisp" in href and not details_url:
                    details_url = href if href.startswith("http") else BASE + href
                continue

            # label-value split on the first colon
            if ":" in ln:
                label, value = ln.split(":", 1)
                label = label.strip()
                value = value.strip()
                # map to our fields
                if label == LABELS["authority"]:
                    rec["authority"] = value
                elif label == LABELS["bis_number"]:
                    rec["bis_number"] = value
                elif label in (LABELS["object"], "Būvobjekts"):
                    rec["object"] = value
                elif label == LABELS["address"]:
                    rec["address"] = value
                elif label == LABELS["construction_type"]:
                    rec["construction_type"] = value
                elif label == LABELS["phase"]:
                    rec["phase"] = value

        if details_url:
            rec["details_url"] = details_url

        # Apply local filters
        if not rec.get("authority") or rec["authority"] not in AUTHORITIES_WHITELIST:
            continue
        if rec.get("phase") and rec["phase"] not in PHASE_KEEP:
            continue
        if rec.get("construction_type") and rec["construction_type"] not in TYPE_KEEP:
            continue

        # ID
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
        last_n = 0

        for n in range(1, PAGES_TOTAL + 1):
            url = LIST_URL.format(page=n)
            try:
                await page.goto(url, wait_until="domcontentloaded", timeout=180000)
            except TimeoutError:
                await page.goto(url, wait_until="domcontentloaded", timeout=180000)

            # Close cookies if shown
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

            rows = parse_listing_blocks(html)

            if not rows:
                empty_streak += 1
                if empty_streak >= 2:
                    last_n = n
                    break
            else:
                empty_streak = 0
                last_n = n
                all_rows.extend(rows)

        await browser.close()

    save_reports(all_rows, last_n)

if __name__ == "__main__":
    import asyncio
    asyncio.run(main())
