import asyncio, os, re, json, pathlib
from datetime import datetime, timezone
from bs4 import BeautifulSoup
from playwright.async_api import async_playwright, TimeoutError as PWTimeout
import pandas as pd

URL = "https://bis.gov.lv/bisp/lv/planned_constructions"

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

INTENT_TYPES = ["Būvatļauja"]  # Ieceres veids

USAGE_CODE_KEEP_PREFIX = "1"
MAX_PAGES_PER_COMBO = int(os.getenv("MAX_PAGES_PER_COMBO", "50"))

# Robust env parsing (shards)
_env = os.environ.get("AUTHORITIES_JSON", "")
try:
    _parsed = json.loads(_env) if _env else []
except json.JSONDecodeError:
    _parsed = []
AUTHORITIES = _parsed or ALL_AUTHORITIES

SHARD_OUTPUT = os.getenv("SHARD_OUTPUT", "").strip()


def row_id(r: dict) -> str:
    if r.get("bis_number"):
        return f"bis:{r['bis_number']}"
    key = "|".join(str(r.get(k, "")) for k in [
        "authority","address","object","phase","construction_type","intention_type","usage_code"
    ])
    import hashlib
    return "h:" + hashlib.sha256(key.encode("utf-8")).hexdigest()[:16]


def parse_table(html: str) -> list[dict]:
    soup = BeautifulSoup(html, "lxml")

    table = soup.find("table")
    if not table:
        # fallback by role attribute if SSR’d
        table = soup.find(attrs={"role": "table"})
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
            "intention_type":    grab(cells, ["Ieceres veids"]),
            "usage_code":        grab(cells, ["Būves lietošanas veids","Lietošanas veids","Lietošanas kods"]),
            "details_url":       link,
        }

        # Exclude when it becomes "Būvdarbi"
        if r["phase"] and r["phase"].strip().lower().startswith("būvdarbi"):
            continue

        # Keep only ĒKAS (1xxx) if present
        code = (r.get("usage_code") or "").strip()
        if code and not code.startswith(USAGE_CODE_KEEP_PREFIX):
            continue

        r["id"] = row_id(r)
        out.append(r)

    return out


async def try_click(page, selector: str, timeout=2000) -> bool:
    try:
        loc = page.locator(selector).first
        if await loc.count() > 0:
            await loc.click(timeout=timeout)
            return True
    except:
        pass
    return False


async def click_next(page) -> bool:
    for sel in [
        "button[aria-label*='Nākam']",
        "a[aria-label*='Nākam']",
        "button:has-text('Nākam')",
        "a:has-text('Nākam')",
    ]:
        if await try_click(page, sel):
            await page.wait_for_timeout(600)
            return True
    return False


async def set_combo(page, name_regex: str, value: str) -> bool:
    """
    Set an autocomplete/combobox by its accessible name (LV label text).
    Works even if <label for=...> isn’t wired.
    """
    try:
        box = page.get_by_role("combobox", name=re.compile(name_regex, re.I)).first
        if await box.count() == 0:
            # fallback to textbox
            box = page.get_by_role("textbox", name=re.compile(name_regex, re.I)).first
        await box.click(timeout=1500)
        await box.fill("")  # clear
        await box.type(value, delay=30)
        # pick exact option
        await page.get_by_role("option", name=value, exact=True).first.click(timeout=2000)
        return True
    except PWTimeout:
        return False
    except:
        return False


async def reset_filters(page):
    # Click "Notīrīt" / "Notīrīt filtrus" if present, otherwise try a generic reset
    for txt in ["Notīrīt", "Notīrīt filtrus", "Atiestatīt"]:
        if await try_click(page, f"button:has-text('{txt}')", timeout=1200):
            await page.wait_for_timeout(400)
            return
    # else: reload the page to reset state
    await page.goto(URL, wait_until="domcontentloaded", timeout=180000)
    await page.wait_for_timeout(400)


async def wait_for_results(page):
    """
    Wait until either the results table has rows OR a 'no data' message is visible.
    """
    # common “no data” phrases
    no_data_patterns = [
        "Nav datu", "Nav atrasts", "Rezultāti nav atrasti",
        "No data", "Nothing found"
    ]
    for _ in range(30):  # up to ~15s
        # Any row?
        if await page.locator("table >> tbody >> tr").count() > 0:
            return "ok"
        # Some apps render rows without <tbody>
        if await page.locator("table tr td").count() > 0:
            return "ok"
        # No data text?
        for pat in no_data_patterns:
            if await page.get_by_text(re.compile(pat, re.I)).count() > 0:
                return "empty"
        await page.wait_for_timeout(500)
    return "unknown"


async def apply_filters(page, authority, phase, ctype, intention):
    await reset_filters(page)

    # Try by role/name instead of label-only
    ok1 = await set_combo(page, r"Būvniecības kontroles institūcija", authority)
    ok2 = await set_combo(page, r"Būvniecības lietas stadija", phase)
    ok3 = await set_combo(page, r"Būvniecības veids", ctype)
    ok4 = await set_combo(page, r"Ieceres veids", intention)

    # Fire search
    clicked = False
    for text in ["Meklēt", "Atrast", "Search"]:
        if await try_click(page, f"button:has-text('{text}')", timeout=1500):
            clicked = True
            break
    if not clicked:
