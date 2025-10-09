import os, json, pathlib
from datetime import datetime, timezone
import pandas as pd

REPORTS = pathlib.Path("reports")
REPORTS.mkdir(parents=True, exist_ok=True)

def safe_load_prev(path: pathlib.Path):
    if not path.exists() or path.stat().st_size == 0:
        return []
    try:
        return pd.read_csv(path, dtype=str).fillna("").to_dict("records")
    except Exception:
        return []

def main():
    # Read shard CSVs from ./shards/*.csv
    shard_dir = pathlib.Path("shards")
    rows = []
    for p in shard_dir.glob("*.csv"):
        df = pd.read_csv(p, dtype=str).fillna("")
        rows.extend(df.to_dict("records"))

    # De-dup by id
    cur_map = {}
    for r in rows:
        cur_map[r.get("id")] = r

    # Diff vs previous snapshot
    prev_rows = safe_load_prev(REPORTS / "latest.csv")
    prev_map = {r.get("id"): r for r in prev_rows}

    new_ids = [i for i in cur_map if i not in prev_map]
    gone_ids = [i for i in prev_map if i not in cur_map]
    changed = []
    for i in set(prev_map).intersection(cur_map):
        a, b = prev_map[i], cur_map[i]
        fields = [k for k in ["phase","construction_type","intention_type","usage_code","address","object"]
                  if (a.get(k,"") != b.get(k,""))]
        if fields:
            changed.append({"id": i, "fields": fields, "before": a, "after": b})

    # Save combined CSVs
    df = pd.DataFrame(list(cur_map.values()))
    df.to_csv(REPORTS / "latest.csv", index=False)
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    df.to_csv(REPORTS / f"{today}.csv", index=False)

    # CHANGELOG
    lines = [
        f"# BIS Plānotie būvdarbi — izmaiņu atskaite ({datetime.now().strftime('%Y-%m-%d %H:%M')})",
        "",
        f"- Kopā rindas: {len(cur_map)}",
        f"- Jauni: {len(new_ids)}",
        f("- Noņemti: " + str(len(gone_ids))),
        f("- Atjaunināti: " + str(len(changed))),
        "",
        "## Jaunie"
    ]
    for i in new_ids:
        r = cur_map[i]
        lines += [f"- **{r.get('authority','?')}** — {r.get('bis_number','?')} — {r.get('address','?')} — {r.get('object','?')} — {r.get('phase','?')} — {r.get('construction_type','?')} — {r.get('intention_type','?')} — {r.get('usage_code','?')}  " +
                  (f"[Saite]({r.get('details_url')})" if r.get('details_url') else "")]
    lines += ["", "## Atjaunināti"]
    for ch in changed:
        before, after = ch["before"], ch["after"]
        lines += [f"- **{after.get('authority','?')}** — {after.get('bis_number','?')} — {after.get('address','?')} — {after.get('object','?')}"]
        for f in ch["fields"]:
            lines += [f"  - {f}: `{before.get(f,'')}` → `{after.get(f,'')}`"]
    lines += ["", "## Noņemti (ID)"] + [f"- {i}" for i in gone_ids]

    (REPORTS / "CHANGELOG.md").write_text("\n".join(lines), encoding="utf-8")

    print(json.dumps({"rows": len(cur_map), "new": len(new_ids), "removed": len(gone_ids), "updated": len(changed)}, ensure_ascii=False))

if __name__ == "__main__":
    main()
