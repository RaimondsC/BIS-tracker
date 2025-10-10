import asyncio, os, re, pathlib, hashlib
from datetime import datetime, timezone
from bs4 import BeautifulSoup
from playwright.async_api import async_playwright
import pandas as pd

URL = "https://bis.gov.lv/bisp/lv/planned_constructions"

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
# ----------------------------------------------------------------

# Page cap (total pages to crawl from page 1). Override in workflow env.
PAGES_TOTAL = int(os.getenv("PAGES_TOTAL", "300"))

def stable_row_id(r: dict) -> str:
    """Prefer BIS number; otherwise hash a few stable fields."""
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
        if a and "bisp" in a["href"]:
            link = a["href"]
            if link.startswith("/"):
                link = "https://bis.gov.lv" + link

        rec = {
            "bis_number":        val(cells, ["Lietas numurs","BIS lietas numurs","Lietas Nr."]),
            "authority":         val(cells, ["Būvniecības kontroles institūcija","Institūcija","Būvvalde"]),
            "address":           val(cells, ["Adrese","Būvobjekta adrese"]),
            "object":            val(cells, ["Būvobjekts","Nosaukums","Objekts"]),
            "phase":             val(cells, ["Būvniecības lietas stadija","Stadija","Statuss"]),
            "construction_type": val(cells, ["Būvniecības veids","Veids"]),
            "details_url":       link,
        }
        # Local filtering
        if rec["phase"] and rec["phase"].strip().lower().startswith("būvdarbi"):
            continue
        if rec["authority"] not in AUTHORITIES_WHITELIST:
            continue
        if rec["phase"] and rec["phase"] not in PHASE_KEEP:
            continue
        if rec["construction_type"] and rec["construction_type"] not in TYPE_KEEP:
            continue

        rec["id"] = stable_row_id(rec)
        out.append(rec)
    return out

async def find_root(page):
    """Return the element scope (page or iframe) that contains the table/pager."""
    if await page.locator("table").count() > 0:
        return page
    for fr in page.frames:
        try:
            if await fr.locator("table").count() > 0 or await fr.get_by_text(re.compile("Nākam", re.I)).count() > 0:
                return fr
        except:
            pass
    return page

async def click_next(root) -> bool:
    for sel in [
        "button[aria-label*='Nākam']",
        "a[aria-label*='Nākam']",
        "button:has-text('Nākam')",
        "a:has-text('Nākam')",
    ]:
        try:
            loc = root.locator(sel).first
            if await loc.count() > 0 and await loc.is_enabled():
                await loc.click(timeout=1500)
                await root.wait_for_timeout(500)
                return True
        except:
            pass
    return False

def safe_load_prev(path: pathlib.Path):
    if not path.exists() or path.stat().st_size == 0:
        return []
    try:
        return pd.read_csv(path, dtype=str).fillna("").to_dict("records")
    except Exception:
        return []

async def main():
    async with async_playwright() as p:
        browser = await p.chromium.launch()
        ctx = await browser.new_context()
        page = await ctx.new_page()

        await page.goto(URL, wait_until="domcontentloaded", timeout=180000)
        # Accept cookies if shown
        for t in ["Apstiprināt", "Apstiprināt visas", "Piekrītu"]:
            try:
                await page.get_by_text(t, exact=False).first.click(timeout=1200)
                break
            except:
                pass
        await page.wait_for_timeout(600)

        root = await find_root(page)

        all_rows, pages = [], 0
        while pages < PAGES_TOTAL:
            html = await root.content()
            rows = parse_table(html)
            all_rows.extend(rows)
            pages += 1
            if not await click_next(root):
                break

        await browser.close()

    # Save snapshot + simple changelog (diff vs previous latest)
    reports = pathlib.Path("reports")
    reports.mkdir(parents=True, exist_ok=True)

    prev_rows = safe_load_prev(reports / "latest.csv")
    prev_map = {r.get("id"): r for r in prev_rows}
    cur_map  = {}
    for r in all_rows:
        cur_map[r.get("id")] = r  # de-dup across pages

    new_ids = [i for i in cur_map if i not in prev_map]
    gone_ids = [i for i in prev_map if i not in cur_map]
    changed = []
    for i in set(prev_map).intersection(cur_map):
        a, b = prev_map[i], cur_map[i]
        diffs = [k for k in ["phase","construction_type","address","object"] if (a.get(k,"") != b.get(k,""))]
        if diffs:
            changed.append({"id": i, "fields": diffs, "before": a, "after": b})

    df = pd.DataFrame(list(cur_map.values()))
    df.to_csv(reports / "latest.csv", index=False)
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    df.to_csv(reports / f"{today}.csv", index=False)

    lines = [
        f"# BIS plānotie darbi — momentuzņēmums ({datetime.now().strftime('%Y-%m-%d %H:%M')})",
        "",
        f"- Pārlapotas lapas: {pages}",
        f"- Rindas pēc filtriem (unikālas): {len(cur_map)}",
        f"- Jauni iepr. salīdzinājumā: {len(new_ids)}",
        f"- Izmainīti: {len(changed)}",
        f"- Noņemti: {len(gone_ids)}",
        "",
        "## Jaunie",
    ]
    for i in new_ids:
        r = cur_map[i]
        lines.append(
            f"- **{r.get('authority','?')}** — {r.get('bis_number','?')} — {r.get('address','?')} — {r.get('object','?')} — "
            f"{r.get('phase','?')} — {r.get('construction_type','?')}  " + (f"[Saite]({r.get('details_url')})" if r.get('details_url') else "")
        )
    lines += ["", "## Izmainīti"]
    for ch in changed:
        before, after = ch["before"], ch["after"]
        lines.append(f"- **{after.get('authority','?')}** — {after.get('bis_number','?')} — {after.get('address','?')} — {after.get('object','?')}")
        for f in ch["fields"]:
            lines.append(f"  - {f}: `{before.get(f,'')}` → `{after.get(f,'')}`")

    (reports / "CHANGELOG.md").write_text("\n".join(lines), encoding="utf-8")
    print({"pages": pages, "rows_unique": len(cur_map)})

if __name__ == "__main__":
    asyncio.run(main())
