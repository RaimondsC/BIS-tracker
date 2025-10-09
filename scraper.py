import asyncio, os, re, json, pathlib
from datetime import datetime, timezone
from bs4 import BeautifulSoup
from playwright.async_api import async_playwright
import pandas as pd

URL = "https://bis.gov.lv/bisp/lv/planned_constructions"

# ------------ FILTER SETS (exact text) ------------
ALL_AUTHORITIES = [
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

# Ieceres veids
INTENT_TYPES = ["Būvatļauja"]

# Page cap per (authority × phase × type × intention)
MAX_PAGES_PER_COMBO = int(os.getenv("MAX_PAGES_PER_COMBO", "50"))

# Sharding: AUTHORITIES_JSON (JSON array) -> limit authorities for this job
_env = os.environ.get("AUTHORITIES_JSON", "")
try:
    _parsed = json.loads(_env) if _env else []
except json.JSONDecodeError:
    _parsed = []
AUTHORITIES = _parsed or ALL_AUTHORITIES

# If set, write shard CSV here and exit (merged later)
SHARD_OUTPUT = os.getenv("SHARD_OUTPUT", "").strip()
# ---------------------------------------------------


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


# --------------- helpers (page/frame) ---------------
async def try_click(root, selector: str, timeout=2000) -> bool:
    try:
        loc = root.locator(selector).first
        if await loc.count() > 0:
            await loc.click(timeout=timeout)
            return True
    except:
        pass
    return False


async def set_combobox_by_name(root, name_regex: str, value: str) -> bool:
    """Type-select option in combobox/textbox by accessible name, under `root` (page or frame)."""
    try:
        box = root.get_by_role("combobox", name=re.compile(name_regex, re.I)).first
        if await box.count() == 0:
            box = root.get_by_role("textbox", name=re.compile(name_regex, re.I)).first
        await box.click(timeout=1500)
        await box.fill("")
        await box.type(value, delay=25)
        await root.get_by_role("option", name=value, exact=True).first.click(timeout=2000)
        return True
    except:
        return False


async def reset_filters(root):
    for txt in ["Notīrīt filtrus", "Notīrīt", "Atiestatīt"]:
        if await try_click(root, f"button:has-text('{txt}')", timeout=1200):
            await root.wait_for_timeout(300)
            return


async def open_advanced(root):
    for txt in ["Izvērstā meklēšana", "Izvērstā", "Izvērst"]:
        if await try_click(root, f"button:has-text('{txt}')", timeout=1200):
            await root.wait_for_timeout(400)
            break
    await try_click(root, f"text=Izvērstā meklēšana", timeout=800)


async def submit_search(root):
    for text in ["Meklēt","Atrast","Search"]:
        if await try_click(root, f"button:has-text('{text}')", timeout=1500):
            await root.wait_for_timeout(700)
            return True
    try:
        await root.get_by_role("combobox").last.press("Enter")
        await root.wait_for_timeout(700)
        return True
    except:
        return False


async def wait_for_results(root):
    no_data_patterns = ["Nav datu", "Nav atrasts", "Rezultāti nav atrasti", "No data"]
    for _ in range(30):  # ~15s
        if await root.locator("table >> tbody >> tr").count() > 0:
            return "ok"
        if await root.locator("table tr td").count() > 0:
            return "ok"
        for pat in no_data_patterns:
            if await root.get_by_text(re.compile(pat, re.I)).count() > 0:
                return "empty"
        await root.wait_for_timeout(500)
    return "unknown"


async def get_search_root(page):
    # Top-level?
    if await page.get_by_text(re.compile("Ātrā meklēšana|Izvērstā meklēšana|Meklēt")).count() > 0:
        return page
    # Look in iframes
    for frame in page.frames:
        try:
            if await frame.get_by_text(re.compile("Ātrā meklēšana|Izvērstā meklēšana|Meklēt")).count() > 0:
                return frame
        except:
            continue
    return page  # fallback
# ----------------------------------------------------


async def apply_filters(root, authority, phase, ctype, intention):
    await reset_filters(root)
    await open_advanced(root)

    okA = await set_combobox_by_name(root, r"Būvniecības kontroles institūcija", authority)
    okB = await set_combobox_by_name(root, r"Būvniecības lietas stadija", phase)
    okC = await set_combobox_by_name(root, r"Būvniecības veids", ctype)
    okD = await set_combobox_by_name(root, r"Ieceres veids", intention)

    submitted = await submit_search(root)
    state = await wait_for_results(root)

    print(f"[FILTERS] A:{okA} B:{okB} C:{okC} D:{okD} submit:{submitted} state:{state} | "
          f"{authority} | {phase} | {ctype} | {intention}")

    return state != "empty"


async def click_next(root) -> bool:
    for sel in [
        "button[aria-label*='Nākam']",
        "a[aria-label*='Nākam']",
        "button:has-text('Nākam')",
        "a:has-text('Nākam')",
    ]:
        if await try_click(root, sel, timeout=1500):
            await root.wait_for_timeout(500)
            return True
    return False


async def scrape_combo(root, authority, phase, ctype, intention):
    has_rows = await apply_filters(root, authority, phase, ctype, intention)
    if not has_rows:
        print(f"[EMPTY] {authority} | {phase} | {ctype} | {intention}")
        return [], 0

    results, pages = [], 0
    while pages < MAX_PAGES_PER_COMBO:
        html = await root.content()
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
        if not await click_next(root):
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

        root = await get_search_root(page)

        all_rows, total_pages = [], 0
        for authority in AUTHORITIES:
            for phase in PHASES:
                for ctype in TYPES:
                    for intention in INTENT_TYPES:
                        chunk, walked = await scrape_combo(root, authority, phase, ctype, intention)
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
        f("- Atjaunināti: " + str(len(changed))),
        f("- Lapu limits vienai kombinācijai: {MAX_PAGES_PER_COMBO}"),
        f("- Faktiski pārlapotas lapas: {total_pages}"),
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
