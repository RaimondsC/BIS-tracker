import asyncio, os, re, json, pathlib
from datetime import datetime, timezone
from bs4 import BeautifulSoup
from playwright.async_api import async_playwright, TimeoutError as PWTimeout
import pandas as pd

URL = "https://bis.gov.lv/bisp/lv/planned_constructions"

# ------------ YOUR FILTER SETS ------------
ALL_AUTHORITIES = [
    "RĪGAS VALSTSPILSĒTAS PAŠVALDĪBAS PAŠVALDĪBAS PILSĒTAS ATTĪSTĪBAS DEPARTAMENTS".replace(" PAŠVALDĪBAS PAŠVALDĪBAS"," PAŠVALDĪBAS"),
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

TYPES = [
    "Atjaunošana",
    "Vienkāršota atjaunošana",
    "Jauna būvniecība",
    "Pārbūve",
    "Vienkāršota pārbūve",
]

# Ieceres veids (Building permit)
INTENT_TYPES = ["Būvatļauja"]

# We must multi-select ALL usage types that start with "1" (ĒKAS + specific 1xxx codes)
USAGE_PREFIX = r"^\s*1"   # regex for option text starting with "1"

# Cap pages per (authority × phase × type × intention)
MAX_PAGES_PER_COMBO = int(os.getenv("MAX_PAGES_PER_COMBO", "50"))

# Sharding (env): AUTHORITIES_JSON is a JSON array of authority strings for this job
_env = os.environ.get("AUTHORITIES_JSON", "")
try:
    _parsed = json.loads(_env) if _env else []
except json.JSONDecodeError:
    _parsed = []
AUTHORITIES = _parsed or ALL_AUTHORITIES

# If set, dump shard rows here and exit (merged later)
SHARD_OUTPUT = os.getenv("SHARD_OUTPUT", "").strip()
# -------------------------------------------


def row_id(r: dict) -> str:
    if r.get("bis_number"):
        return f"bis:{r['bis_number']}"
    key = "|".join(str(r.get(k, "")) for k in [
        "authority","address","object","phase","construction_type","intention_type"
    ])
    import hashlib
    return "h:" + hashlib.sha256(key.encode("utf-8")).hexdigest()[:16]


def parse_table(html: str) -> list[dict]:
    soup = BeautifulSoup(html, "lxml")
    table = soup.find("table") or soup.find(attrs={"role": "table"})
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
            "bis_number":        grab(cells, ["Lietas numurs","BIS lietas numurs","Lietas Nr."]),
            "authority":         grab(cells, ["Būvniecības kontroles institūcija","Institūcija","Būvvalde"]),
            "address":           grab(cells, ["Adrese","Būvobjekta adrese"]),
            "object":            grab(cells, ["Būvobjekts","Nosaukums","Objekts"]),
            "phase":             grab(cells, ["Būvniecības lietas stadija","Stadija","Statuss"]),
            "construction_type": grab(cells, ["Būvniecības veids","Veids"]),
            "details_url":       link,
        }

        # Exclude once it becomes "Būvdarbi"
        if r["phase"] and r["phase"].strip().lower().startswith("būvdarbi"):
            continue

        r["id"] = row_id(r)
        out.append(r)

    return out


# ---------------- UI HELPERS ----------------

async def try_click(page, selector: str, timeout=2000) -> bool:
    try:
        loc = page.locator(selector).first
        if await loc.count() > 0:
            await loc.click(timeout=timeout)
            return True
    except:
        pass
    return False


async def open_advanced(page):
    """
    Ensure 'Izvērstā meklēšana' (Advanced search) is open so all filters are visible.
    We'll click the toggle if we see its text.
    """
    # If there is a button that says "Izvērstā meklēšana", click it once
    for txt in ["Izvērstā meklēšana", "Izvērstā", "Izvērst"]:
        if await try_click(page, f"button:has-text('{txt}')", timeout=1200):
            await page.wait_for_timeout(400)
            break
    # Some UIs use a link/label instead of button:
    await try_click(page, f"text=Izvērstā meklēšana", timeout=800)


async def set_combobox_by_name(page, name_regex: str, value: str) -> bool:
    """Type-select an option in a combobox/textbox by accessible name."""
    try:
        box = page.get_by_role("combobox", name=re.compile(name_regex, re.I)).first
        if await box.count() == 0:
            box = page.get_by_role("textbox", name=re.compile(name_regex, re.I)).first
        await box.click(timeout=1500)
        await box.fill("")
        await box.type(value, delay=25)
        await page.get_by_role("option", name=value, exact=True).first.click(timeout=2000)
        return True
    except:
        return False


async def select_usage_prefix_multi(page, label_regex: str, prefix_regex: str) -> bool:
    """
    Open 'Būves lietošanas veids' control and select ALL options whose text starts with '1'.
    We reopen the dropdown between clicks because many UIs close after a selection.
    """
    ok_any = False
    name_re = re.compile(label_regex, re.I)
    prefix_re = re.compile(prefix_regex)

    # Open the control once to prime it (by role name)
    box = page.get_by_role("combobox", name=name_re).first
    if await box.count() == 0:
        box = page.get_by_role("textbox", name=name_re).first

    if await box.count() == 0:
        # as a fallback, try to find a trigger near the text label
        await try_click(page, f":text('Būves lietošanas veids') >> .. >> button", timeout=1000)

    # Now loop: each time, open dropdown, click ALL visible options starting with 1
    # We do a few passes to catch virtualized lists.
    for _ in range(4):
        try:
            await box.click(timeout=1200)
        except:
            pass

        opts = page.get_by_role("option", name=prefix_re)
        cnt = await opts.count()
        if cnt == 0:
            # try typing "1" to filter the list
            try:
                await box.fill("")
                await box.type("1", delay=25)
                await asyncio.sleep(0.1)
                opts = page.get_by_role("option", name=prefix_re)
                cnt = await opts.count()
            except:
                pass

        clicked_this_round = 0
        for i in range(cnt):
            try:
                # re-query every time because DOM changes after each click
                opt = page.get_by_role("option", name=prefix_re).nth(0)
                if await opt.count() == 0:
                    break
                await opt.click(timeout=1000)
                ok_any = True
                clicked_this_round += 1
                # reopen dropdown for next selection
                try:
                    await box.click(timeout=600)
                except:
                    pass
            except:
                break

        # if nothing more to click, stop looping
        if clicked_this_round == 0:
            break

    return ok_any


async def reset_filters(page):
    # Try “Notīrīt” / “Notīrīt filtrus”
    for txt in ["Notīrīt filtrus", "Notīrīt", "Atiestatīt"]:
        if await try_click(page, f"button:has-text('{txt}')", timeout=1200):
            await page.wait_for_timeout(300)
            return
    # else reload page
    await page.goto(URL, wait_until="domcontentloaded", timeout=180000)
    await page.wait_for_timeout(400)


async def submit_search(page):
    for text in ["Meklēt","Atrast","Search"]:
        if await try_click(page, f"button:has-text('{text}')", timeout=1500):
            await page.wait_for_timeout(700)
            return True
    # fallback: press Enter on any combobox
    try:
        await page.get_by_role("combobox").last.press("Enter")
        await page.wait_for_timeout(700)
        return True
    except:
        return False


async def wait_for_results(page):
    no_data_patterns = ["Nav datu", "Nav atrasts", "Rezultāti nav atrasti", "No data"]
    for _ in range(30):  # ~15s
        if await page.locator("table >> tbody >> tr").count() > 0:
            return "ok"
        if await page.locator("table tr td").count() > 0:
            return "ok"
        for pat in no_data_patterns:
            if await page.get_by_text(re.compile(pat, re.I)).count() > 0:
                return "empty"
        await page.wait_for_timeout(500)
    return "unknown"

# -------------------------------------------


async def apply_filters(page, authority, phase, ctype, intention):
    await reset_filters(page)
    await open_advanced(page)

    okA = await set_combobox_by_name(page, r"Būvniecības kontroles institūcija", authority)
    okB = await set_combobox_by_name(page, r"Būvniecības lietas stadija", phase)
    okC = await set_combobox_by_name(page, r"Būvniecības veids", ctype)
    okD = await set_combobox_by_name(page, r"Ieceres veids", intention)

    # Būves lietošanas veids → select ALL options that start with "1"
    okE = await select_usage_prefix_multi(page, r"Būves lietošanas veids", USAGE_PREFIX)

    submitted = await submit_search(page)
    state = await wait_for_results(page)

    print(f"[FILTERS] A:{okA} B:{okB} C:{okC} D:{okD} E:{okE} submit:{submitted} state:{state} | "
          f"{authority} | {phase} | {ctype} | {intention}")

    return state != "empty"


async def click_next(page) -> bool:
    for sel in [
        "button[aria-label*='Nākam']",
        "a[aria-label*='Nākam']",
        "button:has-text('Nākam')",
        "a:has-text('Nākam')",
    ]:
        if await try_click(page, sel, timeout=1500):
            await page.wait_for_timeout(500)
            return True
    return False


async def scrape_combo(page, authority, phase, ctype, intention):
    has_rows = await apply_filters(page, authority, phase, ctype, intention)
    if not has_rows:
        print(f"[EMPTY] {authority} | {phase} | {ctype} | {intention}")
        return [], 0

    results, pages = [], 0
    while pages < MAX_PAGES_PER_COMBO:
        html = await page.content()
        rows = parse_table(html)
        if not rows:
            break
        for r in rows:
            r["authority"] = authority
            r["phase"] = phase
            r["construction_type"] = ctype
            r["intention_type"] = intention
        results.extend(rows)
        pages += 1
        if not await click_next(page):
            break

    print(f"[OK] {authority} | {phase} | {ctype} | {intention} -> rows:{len(results)} pages:{pages}")
    return results, pages


def safe_load_prev(path: str):
    p = pathlib.Path(path)
    if not p.exists() or p.stat().st_size == 0:
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
                await page.get_by_text(t, exact=False).first.click(timeout=1500)
                break
            except:
                pass
        await page.wait_for_timeout(600)

        all_rows, total_pages = [], 0
        for authority in AUTHORITIES:
            for phase in PHASES:
                for ctype in TYPES:
                    for intention in INTENT_TYPES:
                        chunk, walked = await scrape_combo(page, authority, phase, ctype, intention)
                        all_rows.extend(chunk)
                        total_pages += walked

        await browser.close()

    # Shard mode
    if SHARD_OUTPUT:
        df = pd.DataFrame(all_rows)
        pathlib.Path(SHARD_OUTPUT).parent.mkdir(parents=True, exist_ok=True)
        df.to_csv(SHARD_OUTPUT, index=False)
        print(json.dumps({"mode": "shard", "rows": len(all_rows), "pages": total_pages}, ensure_ascii=False))
        return

    # Single-job mode
    os.makedirs("reports", exist_ok=True)
    prev = safe_load_prev("reports/latest.csv")
    prev_map = {r.get("id"): r for r in prev}
    cur_map = {r.get("id"): r for r in all_rows}

    new_ids = [i for i in cur_map if i not in prev_map]
    gone_ids = [i for i in prev_map if i not in cur_map]
    changed = []
    for i in set(prev_map).intersection(cur_map):
        a, b = prev_map[i], cur_map[i]
        fields = [k for k in ["phase","construction_type","intention_type","address","object"] if (a.get(k,"") != b.get(k,""))]
        if fields:
            changed.append({"id": i, "fields": fields, "before": a, "after": b})

    df = pd.DataFrame(list(cur_map.values()))
    df.to_csv("reports/latest.csv", index=False)
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    df.to_csv(f"reports/{today}.csv", index=False)

    lines = [
        f"# BIS Plānotie būvdarbi — izmaiņu atskaite ({datetime.now().strftime('%Y-%m-%d %H:%M')})",
        "",
        f"- Kopā rindas: {len(cur_map)}",
        f"- Jauni: {len(new_ids)}",
        f"- Noņemti: {len(gone_ids)}",
        f"- Atjaunināti: {len(changed)}",
        f"- Lapu limits vienai kombinācijai: {MAX_PAGES_PER_COMBO}",
        f"- Faktiski pārlapotas lapas: {total_pages}",
        "",
        "## Jaunie",
    ]
    for i in new_ids:
        r = cur_map[i]
        lines += [
            f"- **{r.get('authority','?')}** — {r.get('bis_number','?')} — {r.get('address','?')} — {r.get('object','?')} — "
            f"{r.get('phase','?')} — {r.get('construction_type','?')} — {r.get('intention_type','?')}  "
            + (f"[Saite]({r.get('details_url')})" if r.get('details_url') else "")
        ]
    lines += ["", "## Atjaunināti"]
    for ch in changed:
        before, after = ch["before"], ch["after"]
        lines += [f"- **{after.get('authority','?')}** — {after.get('bis_number','?')} — {after.get('address','?')} — {after.get('object','?')}"]
        for f in ch["fields"]:
            lines += [f"  - {f}: `{before.get(f,'')}` → `{after.get(f,'')}`"]
    lines += ["", "## Noņemti (ID)"] + [f"- {i}" for i in gone_ids]

    with open("reports/CHANGELOG.md", "w", encoding="utf-8") as f:
        f.write("\n".join(lines))

    print(json.dumps({"mode": "full", "rows": len(cur_map), "pages": total_pages}, ensure_ascii=False))


if __name__ == "__main__":
    asyncio.run(main())
