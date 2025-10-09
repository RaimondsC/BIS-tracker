import asyncio, os, re, json
from datetime import datetime, timezone
from bs4 import BeautifulSoup
from playwright.async_api import async_playwright
import pandas as pd

# ---------- CONFIG ----------
URL_LV = "https://bis.gov.lv/bisp/lv/planned_constructions"
URL_EN = "https://bis.gov.lv/bisp/en/planned_constructions"
LANG = os.getenv("LANG", "lv").lower()

# Your filters
ALL_AUTHORITIES = [
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
]

PHASES = [
    "Iecere",
    "Būvniecības ieceres publiskā apspriešana",
    "Projektēšanas nosacījumu izpilde",
    "Būvdarbu uzsākšanas nosacījumu izpilde",
]

CONSTR_TYPES = [
    "Atjaunošana",
    "Vienkāršota atjaunošana",
    "Jauna būvniecība",
    "Pārbūve",
    "Vienkāršota pārbūve",
]

# NEW: Ieceres veids filter (you asked for “Būvatļauja”)
INTENT_TYPES = ["Būvatļauja"]

# Cap pages per combo (you can change via workflow env)
MAX_PAGES_PER_COMBO = int(os.getenv("MAX_PAGES_PER_COMBO", "50"))

# Hard stop in case a combo has insane pagination (safety)
MAX_PAGES_HARD_CAP = int(os.getenv("MAX_PAGES_HARD_CAP", str(MAX_PAGES_PER_COMBO)))
# ---------------------------

def row_id(r: dict) -> str:
    if r.get("bis_number"):
        return f"bis:{r['bis_number']}"
    key = "|".join(str(r.get(k,"")) for k in ["authority","address","object","phase","construction_type","intention_type"])
    import hashlib
    return "h:" + hashlib.sha256(key.encode("utf-8")).hexdigest()[:16]

def parse_table(html: str) -> list[dict]:
    soup = BeautifulSoup(html, "lxml")
    table = soup.find("table")
    out = []
    if not table:
        return out
    headers = [th.get_text(" ", strip=True) for th in table.find_all("th")]

    def grab(cells, names):
        for n in names:
            if n in headers:
                return cells[headers.index(n)]
        return None

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

        r = {
            "bis_number":        grab(cells, ["Lietas numurs","BIS lietas numurs","Lietas Nr.","Case number"]),
            "authority":         grab(cells, ["Būvniecības kontroles institūcija","Institūcija","Būvvalde","Construction control institution"]),
            "address":           grab(cells, ["Adrese","Būvobjekta adrese","Address"]),
            "object":            grab(cells, ["Būvobjekts","Nosaukums","Objekts","Object"]),
            "phase":             grab(cells, ["Būvniecības lietas stadija","Stadija","Statuss","Construction file phase"]),
            "construction_type": grab(cells, ["Būvniecības veids","Veids","Construction type"]),
            # Try to capture Ieceres veids column if present
            "intention_type":    grab(cells, ["Ieceres veids","Initiation type","Type of intention","Intention type"]),
            "details_url":       link,
        }
        # Drop once “Būvdarbi”
        if r["phase"] and r["phase"].strip().lower().startswith("būvdarbi"):
            continue
        r["id"] = row_id(r)
        out.append(r)
    return out

async def click_next(page) -> bool:
    for selector in [
        "button[aria-label*='Nākam']", "a[aria-label*='Nākam']",
        "button:has-text('Nākam')", "a:has-text('Nākam')",
        "button:has-text('Next')", "a:has-text('Next')",
    ]:
        try:
            loc = page.locator(selector).first
            if await loc.count() > 0 and await loc.is_enabled():
                await loc.click(timeout=2000)
                await page.wait_for_timeout(600)
                return True
        except:
            pass
    return False

async def set_filter(page, label_lv, label_en, value):
    # Try LV label first, then EN, then fallback: type and click option by name
    try:
        el = page.get_by_label(label_lv, exact=False)
        await el.fill("")
        await el.fill(value)
        await page.get_by_role("option", name=value).click(timeout=1500)
        return True
    except:
        pass
    try:
        el = page.get_by_label(label_en, exact=False)
        await el.fill("")
        await el.fill(value)
        await page.get_by_role("option", name=value).click(timeout=1500)
        return True
    except:
        pass
    # Last resort: try to click the option if it appears in the DOM
    try:
        await page.get_by_role("option", name=value).click(timeout=1500)
        return True
    except:
        return False

async def apply_filters(page, authority, phase, ctype, intention):
    await set_filter(page, "Būvniecības kontroles institūcija", "Construction control institution", authority)
    await set_filter(page, "Būvniecības lietas stadija", "Construction file phase", phase)
    await set_filter(page, "Būvniecības veids", "Construction type", ctype)
    # NEW: Ieceres veids
    # English label on the site varies; we try several common variants
    await set_filter(page, "Ieceres veids", "Initiation type", intention) or \
    await set_filter(page, "Ieceres veids", "Intention type", intention) or \
    await set_filter(page, "Ieceres veids", "Type of intention", intention)

    # Click search
    for text in ["Meklēt","Atrast","Search","Find"]:
        try:
            await page.get_by_role("button", name=re.compile(text, re.I)).click(timeout=2000)
            break
        except:
            continue
    await page.wait_for_timeout(1000)

async def scrape_combo(page, authority, phase, ctype, intention):
    await apply_filters(page, authority, phase, ctype, intention)

    results = []
    pages = 0
    while pages < min(MAX_PAGES_PER_COMBO, MAX_PAGES_HARD_CAP):
        html = await page.content()
        rows = parse_table(html)
        if not rows:
            break
        # tag combo
        for r in rows:
            r["authority"] = authority
            r["phase"] = phase
            r["construction_type"] = ctype
            r["intention_type"] = intention
        results.extend(rows)
        pages += 1
        moved = await click_next(page)
        if not moved:
            break
    return results, pages

def safe_load_prev(path: str):
    if not os.path.exists(path):
        return []
    # If a previous run created a 0-byte file, treat as empty
    if os.path.getsize(path) == 0:
        return []
    try:
        return pd.read_csv(path, dtype=str).fillna("").to_dict("records")
    except Exception:
        # On any parse error, fall back to empty (avoids EmptyDataError)
        return []

async def main():
    start_url = URL_LV if LANG == "lv" else URL_EN
    async with async_playwright() as p:
        browser = await p.chromium.launch()
        ctx = await browser.new_context()
        page = await ctx.new_page()
        await page.goto(start_url, wait_until="domcontentloaded", timeout=180000)

        # Accept cookies if shown
        for t in ["Apstiprināt", "Apstiprināt visas", "Piekrītu", "Accept", "I agree"]:
            try:
                await page.get_by_text(t, exact=False).first.click(timeout=1500)
                break
            except:
                pass
        await page.wait_for_timeout(800)

        all_rows = []
        total_pages = 0

        for authority in ALL_AUTHORITIES:
            for phase in PHASES:
                for ctype in CONSTR_TYPES:
                    for intention in INTENT_TYPES:
                        chunk, walked = await scrape_combo(page, authority, phase, ctype, intention)
                        all_rows.extend(chunk)
                        total_pages += walked

        await browser.close()

    # Diff
    os.makedirs("reports", exist_ok=True)
    prev = safe_load_prev("reports/latest.csv")
    prev_map = {r.get("id"): r for r in prev}
    cur_map = {r.get("id"): r for r in all_rows}

    new_ids = [i for i in cur_map if i not in prev_map]
    gone_ids = [i for i in prev_map if i not in cur_map]
    changed = []
    for i in set(prev_map).intersection(cur_map):
        a, b = prev_map[i], cur_map[i]
        fields = [k for k in ["phase","construction_type","intention_type","address","object"]
                  if (a.get(k,"") != b.get(k,""))]
        if fields:
            changed.append({"id": i, "fields": fields, "before": a, "after": b})

    # Save CSVs
    df = pd.DataFrame(list(cur_map.values()))
    df.to_csv("reports/latest.csv", index=False)
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    df.to_csv(f"reports/{today}.csv", index=False)

    # Changelog
    lines = [
        f"# BIS Plānotie būvdarbi — izmaiņu atskaite ({datetime.now().strftime('%Y-%m-%d %H:%M')})",
        "",
        f"- Kopā rindas: {len(cur_map)}",
        f"- Jauni: {len(new_ids)}",
        f"- Noņemti: {len(gone_ids)}",
        f"- Atjaunināti: {len(changed)}",
        f"- Katrā kombinācijā lapu limits: {MAX_PAGES_PER_COMBO}",
        f"- Faktiski pārlapotas lapas: {total_pages}",
        "",
        "## Jaunie"
    ]
    for i in new_ids:
        r = cur_map[i]
        lines += [f"- **{r.get('authority','?')}** — {r.get('bis_number','?')} — {r.get('address','?')} — {r.get('object','?')} — {r.get('phase','?')} — {r.get('construction_type','?')} — {r.get('intention_type','?')}  " +
                  (f"[Saite]({r.get('details_url')})" if r.get('details_url') else "")]
    lines += ["", "## Atjaunināti"]
    for ch in changed:
        before, after = ch["before"], ch["after"]
        lines += [f"- **{after.get('authority','?')}** — {after.get('bis_number','?')} — {after.get('address','?')} — {after.get('object','?')}"]
        for f in ch["fields"]:
            lines += [f"  - {f}: `{before.get(f,'')}` → `{after.get(f,'')}`"]
    lines += ["", "## Noņemti (ID)"] + [f"- {i}" for i in gone_ids]

    with open("reports/CHANGELOG.md", "w", encoding="utf-8") as f:
        f.write("\n".join(lines))

    print(json.dumps({
        "total_rows": len(cur_map),
        "new": len(new_ids),
        "removed": len(gone_ids),
        "updated": len(changed),
        "pages_per_combo_cap": MAX_PAGES_PER_COMBO,
        "pages_walked_total": total_pages
    }, ensure_ascii=False))

if __name__ == "__main__":
    asyncio.run(main())
