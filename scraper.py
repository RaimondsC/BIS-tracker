import asyncio, os, re, json, pathlib
from datetime import datetime, timezone
from bs4 import BeautifulSoup
from playwright.async_api import async_playwright
import pandas as pd

# ===================== CONFIG =====================
# LV-only page (EN disabled to avoid inconsistencies)
URL = "https://bis.gov.lv/bisp/lv/planned_constructions"

# Authorities (exact strings)
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

# Construction file phase
PHASES = [
    "Iecere",
    "Būvniecības ieceres publiskā apspriešana",
    "Projektēšanas nosacījumu izpilde",
    "Būvdarbu uzsākšanas nosacījumu izpilde",
]

# Construction type
TYPES = [
    "Atjaunošana",
    "Vienkāršota atjaunošana",
    "Jauna būvniecība",
    "Pārbūve",
    "Vienkāršota pārbūve",
]

# Ieceres veids
INTENT_TYPES = ["Būvatļauja"]

# Keep only usage codes that start with "1" (ĒKAS); if column missing, keep row.
USAGE_CODE_KEEP_PREFIX = "1"

# Page limit per (authority × phase × type × intention); can override via env
MAX_PAGES_PER_COMBO = int(os.getenv("MAX_PAGES_PER_COMBO", "50"))

# --- Shard controls ---
# Robustly parse AUTHORITIES_JSON (JSON array) or fall back to ALL_AUTHORITIES
_env = os.environ.get("AUTHORITIES_JSON", "")
try:
    _parsed = json.loads(_env) if _env else []
except json.JSONDecodeError:
    _parsed = []
AUTHORITIES = _parsed or ALL_AUTHORITIES

# If set, write shard CSV to this path and exit (merge happens later)
SHARD_OUTPUT = os.getenv("SHARD_OUTPUT", "").strip()
# ==================================================


def row_id(r: dict) -> str:
    """Stable ID for diffing; prefer BIS number if present."""
    if r.get("bis_number"):
        return f"bis:{r['bis_number']}"
    key = "|".join(
        str(r.get(k, ""))
        for k in [
            "authority",
            "address",
            "object",
            "phase",
            "construction_type",
            "intention_type",
            "usage_code",
        ]
    )
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
            "bis_number":        grab(cells, ["Lietas numurs", "BIS lietas numurs", "Lietas Nr."]),
            "authority":         grab(cells, ["Būvniecības kontroles institūcija", "Institūcija", "Būvvalde"]),
            "address":           grab(cells, ["Adrese", "Būvobjekta adrese"]),
            "object":            grab(cells, ["Būvobjekts", "Nosaukums", "Objekts"]),
            "phase":             grab(cells, ["Būvniecības lietas stadija", "Stadija", "Statuss"]),
            "construction_type": grab(cells, ["Būvniecības veids", "Veids"]),
            "intention_type":    grab(cells, ["Ieceres veids"]),
            "usage_code":        grab(cells, ["Būves lietošanas veids", "Lietošanas veids", "Lietošanas kods"]),
            "details_url":       link,
        }

        # Exclude once it becomes "Būvdarbi" (not actual anymore)
        if r["phase"] and r["phase"].strip().lower().startswith("būvdarbi"):
            continue

        # Keep only ĒKAS (1xxx) if the code is present
        code = (r.get("usage_code") or "").strip()
        if code and not code.startswith(USAGE_CODE_KEEP_PREFIX):
            continue

        r["id"] = row_id(r)
        out.append(r)

    return out


async def click_next(page) -> bool:
    """Try to move to next page; returns True if succeeded."""
    for selector in [
        "button[aria-label*='Nākam']",
        "a[aria-label*='Nākam']",
        "button:has-text('Nākam')",
        "a:has-text('Nākam')",
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


async def set_filter(page, label_lv: str, value: str) -> bool:
    """Fill a LV-labeled combobox and choose the option by visible name."""
    try:
        el = page.get_by_label(label_lv, exact=False)
        await el.fill("")
        await el.fill(value)
        await page.get_by_role("option", name=value).click(timeout=1500)
        return True
    except:
        return False


async def apply_filters(page, authority, phase, ctype, intention):
    await set_filter(page, "Būvniecības kontroles institūcija", authority)
    await set_filter(page, "Būvniecības lietas stadija", phase)
    await set_filter(page, "Būvniecības veids", ctype)
    await set_filter(page, "Ieceres veids", intention)

    # Click search
    for text in ["Meklēt", "Atrast"]:
        try:
            await page.get_by_role("button", name=re.compile(text, re.I)).click(timeout=2000)
            break
        except:
            continue
    await page.wait_for_timeout(800)


async def scrape_combo(page, authority, phase, ctype, intention):
    """Scrape all rows for a specific filter combination, up to page cap."""
    await apply_filters(page, authority, phase, ctype, intention)

    results, pages = [], 0
    while pages < MAX_PAGES_PER_COMBO:
        html = await page.content()
        rows = parse_table(html)
        if not rows:
            break

        # tag the combo on each row
        for r in rows:
            r["authority"] = authority
            r["phase"] = phase
            r["construction_type"] = ctype
            r["intention_type"] = intention

        results.extend(rows)
        pages += 1

        if not await click_next(page):
            break

    return results, pages


def safe_load_prev(path: str):
    """Read CSV to dict list; return [] if missing/empty/corrupt."""
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

    # If shard mode is enabled, write shard CSV and exit (merged later)
    if SHARD_OUTPUT:
        df = pd.DataFrame(all_rows)
        pathlib.Path(SHARD_OUTPUT).parent.mkdir(parents=True, exist_ok=True)
        df.to_csv(SHARD_OUTPUT, index=False)
        print(json.dumps({"mode": "shard", "rows": len(all_rows), "pages": total_pages}, ensure_ascii=False))
        return

    # Single-job mode: build diff and write reports/
    os.makedirs("reports", exist_ok=True)
    prev = safe_load_prev("reports/latest.csv")
    prev_map = {r.get("id"): r for r in prev}
    cur_map = {r.get("id"): r for r in all_rows}

    new_ids = [i for i in cur_map if i not in prev_map]
    gone_ids = [i for i in prev_map if i not in cur_map]
    changed = []
    for i in set(prev_map).intersection(cur_map):
        a, b = prev_map[i], cur_map[i]
        fields = [
            k
            for k in ["phase", "construction_type", "intention_type", "usage_code", "address", "object"]
            if (a.get(k, "") != b.get(k, ""))
        ]
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
            f"{r.get('phase','?')} — {r.get('construction_type','?')} — {r.get('intention_type','?')} — {r.get('usage_code','?')}  "
            + (f"[Saite]({r.get('details_url')})" if r.get('details_url') else "")
        ]
    lines += ["", "## Atjaunināti"]
    for ch in changed:
        before, after = ch["before"], ch["after"]
        lines += [
            f"- **{after.get('authority','?')}** — {after.get('bis_number','?')} — {after.get('address','?')} — {after.get('object','?')}"
        ]
        for f in ch["fields"]:
            lines += [f"  - {f}: `{before.get(f,'')}` → `{after.get(f,'')}`"]
    lines += ["", "## Noņemti (ID)"] + [f"- {i}" for i in gone_ids]

    with open("reports/CHANGELOG.md", "w", encoding="utf-8") as f:
        f.write("\n".join(lines))

    print(json.dumps({"mode": "full", "rows": len(cur_map), "pages": total_pages}, ensure_ascii=False))


if __name__ == "__main__":
    asyncio.run(main())
