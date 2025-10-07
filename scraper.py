import asyncio, hashlib, os, re, json
from datetime import datetime, timezone
from bs4 import BeautifulSoup
from playwright.async_api import async_playwright
import pandas as pd

URL = "https://bis.gov.lv/bisp/lv/planned_constructions"

# === YOUR FILTERS ===
ALLOWED_AUTHORITIES = {
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

ALLOWED_STAGES = {
    "Iecere",
    "Būvniecības ieceres publiskā apspriešana",
    "Projektēšanas nosacījumu izpilde",
    "Būvdarbu uzsākšanas nosacījumu izpilde",
    # NOTE: when it becomes "Būvdarbi", we will drop it as non-actual
}

ALLOWED_CONSTRUCTION_TYPES = {"Atjaunošana", "Jauna būvniecība", "Pārbūve"}

def usage_code_allowed(code: str | None) -> bool:
    """Exclude usage starting with '2' (inženierbūves) when present."""
    if not code:
        return True
    return not str(code).strip().startswith("2")

# ====================

def row_hash(d: dict) -> str:
    # Stable hash to track a listing even if order changes
    keys = ["bis_number","authority","address","object","stage","construction_type","usage_code","published"]
    s = "|".join(str(d.get(k,"")).strip() for k in keys)
    import hashlib
    return hashlib.sha256(s.encode("utf-8")).hexdigest()[:16]

async def fetch_all_pages():
    """Use a headless browser (Playwright) to accept cookies and click through all pagination pages.
    Returns a list of BeautifulSoup fragments (one per page)."""
    soups = []
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        ctx = await browser.new_context()
        page = await ctx.new_page()
        await page.goto(URL, wait_until="domcontentloaded", timeout=90000)

        # Try to accept cookies if a banner exists
        for text in ["Apstiprināt", "Piekrītu", "Akceptēt"]:
            try:
                await page.get_by_text(text, exact=False).first.click(timeout=2000)
                break
            except:
                pass

        # Give the client-side app time to render
        await page.wait_for_timeout(2500)

        # Helper: capture current page HTML as soup
        async def capture():
            html = await page.content()
            soups.append(BeautifulSoup(html, "lxml"))

        # Capture first page
        await capture()

        # Try to find and click "Next" until disabled/absent
        # We try common Latvian labels and icon roles
        while True:
            clicked = False
            for sel in [
                "text=Nākamā", "text=Nākošā", "text=Next",
                "button[aria-label='Nākamā']", "a[aria-label='Nākamā']",
                "button[rel='next']", "a[rel='next']",
            ]:
                try:
                    locator = page.locator(sel)
                    if await locator.count() > 0 and await locator.first.is_enabled():
                        await locator.first.click()
                        await page.wait_for_timeout(1200)
                        await capture()
                        clicked = True
                        break
                except:
                    pass
            if not clicked:
                break

        await browser.close()
    return soups

def text_of(el):
    return el.get_text(" ", strip=True) if el else None

def parse_one_page(soup: BeautifulSoup):
    """Parse a page into a list of dicts. Handles tables or card layouts."""
    out = []

    # Strategy A: table with headers
    table = soup.find("table")
    if table:
        headers = [text_of(th) for th in table.find_all("th")]
        for tr in table.find_all("tr"):
            tds = tr.find_all("td")
            if not tds:
                continue
            cells = [text_of(td) for td in tds]
            def grab(names):
                for n in names:
                    if n in headers:
                        return cells[headers.index(n)]
                return None

            # Try to locate a link for details (BIS case page)
            link = None
            a = tr.find("a", href=True)
            if a and "bisp" in a["href"]:
                link = a["href"]
                if link.startswith("/"):
                    link = "https://bis.gov.lv" + link

            record = {
                "bis_number":     grab(["Lietas numurs","BIS lietas numurs","Lietas Nr."]),
                "authority":      grab(["Būvniecības kontroles institūcija","Institūcija","Būvvalde"]),
                "address":        grab(["Adrese","Būvobjekta adrese"]),
                "object":         grab(["Būvobjekts","Nosaukums","Objekts"]),
                "stage":          grab(["Būvniecības lietas stadija","Stadija","Statuss"]),
                "construction_type": grab(["Būvniecības veids","Veids"]),
                "usage_code":     grab(["Būves lietošanas veids","Lietošanas veids","Lietošanas kods"]),
                "published":      grab(["Publicēts","Datums"]),
                "details_url":    link,
            }
            record["id"] = row_hash(record)
            out.append(record)

    # Strategy B: card/list layout (fallback)
    if not out:
        cards = soup.select("article, li, div.card, div.row, div.item")
        for c in cards:
            txt = " ".join(c.stripped_strings)
            if len(txt) < 40:
                continue
            # Best-effort extraction with regex labels
            def find(label):
                m = re.search(rf"{label}\s*[:\-]\s*([^\|]+)", txt, re.I)
                return m.group(1).strip() if m else None
            a = c.find("a", href=True)
            link = a["href"] if a else None
            if link and link.startswith("/"):
                link = "https://bis.gov.lv" + link

            record = {
                "bis_number":        find(r"(Lietas\s*nr\.?|Lietas numurs|BIS lietas numurs)"),
                "authority":         find(r"(Būvniecības kontroles institūcija|Institūcija|Būvvalde)"),
                "address":           find(r"(Adrese|Būvobjekta adrese)"),
                "object":            find(r"(Būvobjekts|Nosaukums|Objekts)"),
                "stage":             find(r"(Būvniecības lietas stadija|Stadija|Statuss)"),
                "construction_type": find(r"(Būvniecības veids|Veids)"),
                "usage_code":        find(r"(Būves lietošanas veids|Lietošanas veids|Lietošanas kods)"),
                "published":         find(r"(Publicēts|Datums)"),
                "details_url":       link,
            }
            if any(record.values()):
                record["id"] = row_hash(record)
                out.append(record)

    # Deduplicate by id per page
    return list({r["id"]: r for r in out}.values())

def apply_filters(rows: list[dict]) -> list[dict]:
    filtered = []
    for r in rows:
        # Drop once stage becomes "Būvdarbi"
        if r.get("stage") == "Būvdarbi":
            continue
        if r.get("authority") not in ALLOWED_AUTHORITIES:
            continue
        if r.get("stage") not in ALLOWED_STAGES:
            continue
        if r.get("construction_type") not in ALLOWED_CONSTRUCTION_TYPES:
            continue
        if not usage_code_allowed(r.get("usage_code")):
            continue
        filtered.append(r)
    return filtered

def load_prev():
    p = "reports/latest.csv"
    if os.path.exists(p):
        return pd.read_csv(p, dtype=str).fillna("").to_dict("records")
    return []

def save_snap(rows: list[dict]):
    os.makedirs("reports", exist_ok=True)
    df = pd.DataFrame(rows)
    df.to_csv("reports/latest.csv", index=False)
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    df.to_csv(f"reports/{today}.csv", index=False)

def diff(prev: list[dict], curr: list[dict]):
    prev_map = {r["id"]: r for r in prev}
    curr_map = {r["id"]: r for r in curr}
    new_ids = [i for i in curr_map if i not in prev_map]
    gone_ids = [i for i in prev_map if i not in curr_map]
    changed = []
    for i in set(prev_map).intersection(curr_map):
        a, b = prev_map[i], curr_map[i]
        changed_fields = [k for k in ["stage","construction_type","usage_code","address","object","published"]
                          if (a.get(k,"") != b.get(k,""))]
        if changed_fields:
            changed.append({"id": i, "fields": changed_fields, "before": a, "after": b})
    return new_ids, gone_ids, changed

def write_changelog(new_ids, gone_ids, changed, curr):
    cmap = {r["id"]: r for r in curr}
    ts = datetime.now().strftime("%Y-%m-%d %H:%M")
    lines = [
        f"# BIS Plānotie būvdarbi — izmaiņu atskaite ({ts})",
        "",
        f"- Jauni ieraksti: {len(new_ids)}",
        f"- Noņemti (vairs neatbilst filtriem vai pazuduši): {len(gone_ids)}",
        f"- Atjaunināti: {len(changed)}",
        "",
        "## Jaunie"
    ]
    for i in new_ids:
        r = cmap[i]
        lines += [f"- **{r.get('authority','?')}** — {r.get('bis_number','?')} — {r.get('address','?')} — {r.get('object','?')} — {r.get('stage','?')} — {r.get('construction_type','?')} — {r.get('published','?')}  " +
                  (f"[Saite]({r.get('details_url')})" if r.get("details_url") else "")]
    lines += ["", "## Atjaunināti"]
    for ch in changed:
        before, after = ch["before"], ch["after"]
        lines += [f"- **{after.get('authority','?')}** — {after.get('bis_number','?')} — {after.get('address','?')} — {after.get('object','?')}"]
        for f in ch["fields"]:
            lines += [f"  - {f}: `{before.get(f,'')}` → `{after.get(f,'')}`"]
    lines += ["", "## Noņemti (ID)"]
    for i in gone_ids:
        lines += [f"- {i}"]
    with open("reports/CHANGELOG.md", "w", encoding="utf-8") as f:
        f.write("\n".join(lines))

async def main():
    soups = await fetch_all_pages()
    all_rows = []
    for s in soups:
        all_rows.extend(parse_one_page(s))
    # Keep only filtered rows
    cur = apply_filters(all_rows)
    # Diff
    prev = load_prev()
    new_ids, gone_ids, changed = diff(prev, cur)
    # Save and report
    save_snap(cur)
    write_changelog(new_ids, gone_ids, changed, cur)
    print(json.dumps({"total": len(cur), "new": len(new_ids), "removed": len(gone_ids), "updated": len(changed)}, ensure_ascii=False))

if __name__ == "__main__":
    asyncio.run(main())
