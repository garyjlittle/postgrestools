#!/usr/bin/env python3
"""
Compare two DB state JSON snapshots produced by show_xray_db_config.py --output.

Usage:
    python3 compare_db_state.py pre-run-state.json post-run-state.json [-o diff.json]
"""

import argparse
import json
import sys
from typing import List, Tuple, Optional
from tabulate import tabulate


class Colors:
    RED = "\033[0;31m"
    GREEN = "\033[0;32m"
    MAGENTA = "\033[0;35m"
    BLUE = "\033[0;34m"
    BOLD_BLUE = "\033[1;34m"
    BOLD = "\033[1m"
    NC = "\033[0m"


# ── formatting helpers (match show_xray_db_config.py style) ──────────


def print_table(headers: List[str], rows: List[List[str]]):
    if not rows:
        return
    output = tabulate(rows, headers=headers, tablefmt="simple")
    for line in output.splitlines():
        print(f"  {line}")


def print_kv(pairs: List[Tuple[str, str]], indent: int = 2):
    if not pairs:
        return
    max_key = max(len(k) for k, _ in pairs)
    for key, val in pairs:
        print(f"{' ' * indent}{key:<{max_key}}  {val}")


def _c(color: str, msg: str) -> str:
    return f"{color}{msg}{Colors.NC}"


def _section(title: str):
    print(f"\n{_c(Colors.BOLD_BLUE, f'── {title} ──')}")


def pretty_bytes(b) -> str:
    b = float(b)
    for unit in ("bytes", "kB", "MB", "GB", "TB"):
        if abs(b) < 1024:
            return f"{b:.1f} {unit}" if unit != "bytes" else f"{int(b)} bytes"
        b /= 1024.0
    return f"{b:.1f} PB"


def delta_str(val: float) -> str:
    sign = "+" if val >= 0 else ""
    return f"{sign}{pretty_bytes(val)}"


def pct_str(pre: float, post: float) -> str:
    if pre == 0:
        return "(new)" if post > 0 else ""
    pct = ((post - pre) / pre) * 100
    sign = "+" if pct >= 0 else ""
    return f"{sign}{pct:.1f}%"


def color_delta(val: float) -> str:
    """Color a byte-delta: red for growth, green for shrink."""
    text = delta_str(val)
    if val > 0:
        return _c(Colors.RED, text)
    elif val < 0:
        return _c(Colors.GREEN, text)
    return text


def color_pct(pre: float, post: float) -> str:
    text = pct_str(pre, post)
    diff = post - pre
    if diff > 0:
        return _c(Colors.RED, text)
    elif diff < 0:
        return _c(Colors.GREEN, text)
    return text


_SIZE_UNITS = {
    "bytes": 1, "kb": 1024, "mb": 1024**2,
    "gb": 1024**3, "tb": 1024**4, "pb": 1024**5,
}


def parse_size(s: str) -> Optional[int]:
    """Parse a human-readable size string like '180 GB' back to bytes."""
    if not s or s == "?":
        return None
    parts = s.strip().split()
    if len(parts) != 2:
        return None
    try:
        num = float(parts[0])
        unit = parts[1].lower()
        if unit in _SIZE_UNITS:
            return int(num * _SIZE_UNITS[unit])
    except (ValueError, IndexError):
        pass
    return None


# ── comparison logic ─────────────────────────────────────────────────


def load_json(path: str) -> dict:
    try:
        with open(path) as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError) as e:
        print(f"Error reading {path}: {e}", file=sys.stderr)
        sys.exit(1)


def diff_database_size(pre: dict, post: dict) -> Optional[dict]:
    pre_bytes = None
    post_bytes = None
    pre_label = None
    post_label = None

    if "database_size" in pre:
        pre_bytes = pre["database_size"].get("bytes")
        pre_label = pre["database_size"].get("pretty", "?")
    if "database_size" in post:
        post_bytes = post["database_size"].get("bytes")
        post_label = post["database_size"].get("pretty", "?")

    if pre_bytes is None or post_bytes is None:
        return None

    delta = post_bytes - pre_bytes
    result = {
        "pre_bytes": pre_bytes,
        "post_bytes": post_bytes,
        "delta_bytes": delta,
        "pre_pretty": pre_label,
        "post_pretty": post_label,
        "delta_pretty": delta_str(delta),
        "pct": pct_str(pre_bytes, post_bytes),
    }

    _section("Database Size")
    print_table(
        ["", "Size", "Bytes"],
        [
            ["Pre", pre_label, f"{pre_bytes:,}"],
            ["Post", post_label, f"{post_bytes:,}"],
            ["Delta", color_delta(delta), color_pct(pre_bytes, post_bytes)],
        ],
    )

    return result


def diff_tablespace(pre: dict, post: dict) -> Optional[dict]:
    pre_ts = pre.get("tablespace", {})
    post_ts = post.get("tablespace", {})

    if not pre_ts and not post_ts:
        return None

    result = {}
    rows = []

    ts_size_pre = pre_ts.get("ts_size")
    ts_size_post = post_ts.get("ts_size")
    if ts_size_pre and ts_size_post:
        result["ts_size_pre"] = ts_size_pre
        result["ts_size_post"] = ts_size_post
        pre_b = parse_size(ts_size_pre)
        post_b = parse_size(ts_size_post)
        ts_delta = ""
        ts_pct = ""
        if pre_b is not None and post_b is not None:
            delta = post_b - pre_b
            ts_delta = color_delta(delta)
            ts_pct = color_pct(pre_b, post_b)
        rows.append(["TS Size", ts_size_pre, ts_size_post, ts_delta, ts_pct])

    pre_disk = pre_ts.get("disk", {})
    post_disk = post_ts.get("disk", {})

    if not pre_disk and not post_disk and not ts_size_pre:
        return None

    if pre_disk and post_disk:
        result["disk_pre"] = pre_disk
        result["disk_post"] = post_disk
        pre_used = pre_disk.get("used", "?")
        post_used = post_disk.get("used", "?")
        total = pre_disk.get("size", "?")
        pre_pct_s = pre_disk.get("use_pct", "?")
        post_pct_s = post_disk.get("use_pct", "?")

        disk_delta = ""
        disk_pct = ""
        pre_used_b = parse_size(pre_used.replace("G", " GB").replace("T", " TB")) if pre_used != "?" else None
        post_used_b = parse_size(post_used.replace("G", " GB").replace("T", " TB")) if post_used != "?" else None
        if pre_used_b is not None and post_used_b is not None:
            disk_delta = color_delta(post_used_b - pre_used_b)
        try:
            pre_pct_f = float(pre_pct_s.rstrip("%"))
            post_pct_f = float(post_pct_s.rstrip("%"))
            pct_delta = post_pct_f - pre_pct_f
            sign = "+" if pct_delta >= 0 else ""
            disk_pct = f"{sign}{pct_delta:.0f}%"
        except (ValueError, AttributeError):
            pass

        rows.append(["Disk (df)", f"{pre_used} / {total} ({pre_pct_s})", f"{post_used} / {total} ({post_pct_s})", disk_delta, disk_pct])

    _section("Tablespace / Disk Usage")
    print_table(["", "Pre", "Post", "Delta", "Change"], rows)
    return result if result else None


def _build_table_map(data: dict) -> Tuple[dict, bool]:
    """Build a {schema.table -> entry} map, preferring all_tables (exact bytes)
    and falling back to top_tables (parsed human-readable sizes)."""
    if data.get("all_tables"):
        return {
            f"{t['schema']}.{t['table']}": t
            for t in data["all_tables"]
        }, False

    if data.get("top_tables"):
        result = {}
        for t in data["top_tables"]:
            sz = parse_size(t.get("size", "")) or 0
            result[f"{t['schema']}.{t['table']}"] = {
                **t, "size_bytes": sz,
            }
        return result, True

    return {}, False


def diff_tables(pre: dict, post: dict) -> Optional[dict]:
    pre_tables, pre_approx = _build_table_map(pre)
    post_tables, post_approx = _build_table_map(post)
    approx = pre_approx or post_approx

    if not pre_tables and not post_tables:
        return None

    all_keys = sorted(set(pre_tables) | set(post_tables))
    rows = []
    json_rows = []

    for key in all_keys:
        pt = pre_tables.get(key)
        po = post_tables.get(key)

        pre_sz = pt.get("size_bytes", 0) if pt else 0
        post_sz = po.get("size_bytes", 0) if po else 0
        sz_delta = post_sz - pre_sz

        pre_rows = pt.get("rows", 0) if pt else 0
        post_rows = po.get("rows", 0) if po else 0
        row_delta = post_rows - pre_rows

        status = ""
        if pt is None:
            status = _c(Colors.GREEN, "[NEW]")
        elif po is None:
            status = _c(Colors.MAGENTA, "[DROPPED]")

        json_entry = {
            "name": key,
            "pre_bytes": pre_sz,
            "post_bytes": post_sz,
            "delta_bytes": sz_delta,
            "pre_rows": pre_rows,
            "post_rows": post_rows,
            "delta_rows": row_delta,
        }
        if pt is None:
            json_entry["status"] = "new"
        elif po is None:
            json_entry["status"] = "dropped"

        size_change = color_pct(pre_sz, post_sz) if pt and po else ""

        rows.append((abs(sz_delta), [
            key,
            pretty_bytes(pre_sz) if pt else "-",
            pretty_bytes(post_sz) if po else "-",
            color_delta(sz_delta),
            size_change,
            f"{pre_rows:,}" if pt else "-",
            f"{post_rows:,}" if po else "-",
            f"{'+' if row_delta >= 0 else ''}{row_delta:,}",
            status,
        ]))
        json_rows.append((abs(sz_delta), json_entry))

    rows.sort(key=lambda x: x[0], reverse=True)
    json_rows.sort(key=lambda x: x[0], reverse=True)

    label = "Table Changes (top-N only, sizes approximate)" if approx else "Table Changes"
    _section(label)
    print_table(
        ["Table", "Pre Size", "Post Size", "Size Delta", "Change", "Pre Rows", "Post Rows", "Row Delta", ""],
        [r[1] for r in rows],
    )

    result = {"tables": [r[1] for r in json_rows]}
    if approx:
        result["approximate"] = True
    return result


def _build_index_map(data: dict) -> Tuple[dict, bool]:
    """Build an index map, preferring all_indexes (exact bytes) and falling
    back to top_indexes (parsed human-readable sizes, no schema key)."""
    if data.get("all_indexes"):
        return {
            f"{i['schema']}.{i['index']}": i
            for i in data["all_indexes"]
        }, False

    if data.get("top_indexes"):
        result = {}
        for i in data["top_indexes"]:
            sz = parse_size(i.get("size", "")) or 0
            key = i["index"]
            result[key] = {**i, "size_bytes": sz}
        return result, True

    return {}, False


def diff_indexes(pre: dict, post: dict) -> Optional[dict]:
    pre_idx, pre_approx = _build_index_map(pre)
    post_idx, post_approx = _build_index_map(post)
    approx = pre_approx or post_approx

    if not pre_idx and not post_idx:
        return None

    all_keys = sorted(set(pre_idx) | set(post_idx))
    rows = []
    json_rows = []

    for key in all_keys:
        pi = pre_idx.get(key)
        po = post_idx.get(key)

        pre_sz = pi.get("size_bytes", 0) if pi else 0
        post_sz = po.get("size_bytes", 0) if po else 0
        sz_delta = post_sz - pre_sz

        on_table = (po or pi).get("on_table", "?")

        status = ""
        if pi is None:
            status = _c(Colors.GREEN, "[NEW]")
        elif po is None:
            status = _c(Colors.MAGENTA, "[DROPPED]")

        json_entry = {
            "name": key,
            "on_table": on_table,
            "pre_bytes": pre_sz,
            "post_bytes": post_sz,
            "delta_bytes": sz_delta,
        }
        if pi is None:
            json_entry["status"] = "new"
        elif po is None:
            json_entry["status"] = "dropped"

        size_change = color_pct(pre_sz, post_sz) if pi and po else ""

        rows.append((abs(sz_delta), [
            key,
            on_table,
            pretty_bytes(pre_sz) if pi else "-",
            pretty_bytes(post_sz) if po else "-",
            color_delta(sz_delta),
            size_change,
            status,
        ]))
        json_rows.append((abs(sz_delta), json_entry))

    rows.sort(key=lambda x: x[0], reverse=True)
    json_rows.sort(key=lambda x: x[0], reverse=True)

    label = "Index Changes (top-N only, sizes approximate)" if approx else "Index Changes"
    _section(label)
    print_table(
        ["Index", "On Table", "Pre Size", "Post Size", "Size Delta", "Change", ""],
        [r[1] for r in rows],
    )

    result = {"indexes": [r[1] for r in json_rows]}
    if approx:
        result["approximate"] = True
    return result


def _sum_from_top(data: dict, tables_key: str, indexes_key: str) -> Tuple[int, int, int]:
    """Sum size_bytes and rows from top-N table/index lists (parsed or exact)."""
    tb = sum(
        t.get("size_bytes", 0) or (parse_size(t.get("size", "")) or 0)
        for t in data.get(tables_key, [])
    )
    tr = sum(t.get("rows", 0) for t in data.get(tables_key, []))
    ib = sum(
        i.get("size_bytes", 0) or (parse_size(i.get("size", "")) or 0)
        for i in data.get(indexes_key, [])
    )
    return tb, tr, ib


def diff_totals(pre: dict, post: dict) -> Optional[dict]:
    pre_tt = pre.get("tables_total", {})
    post_tt = post.get("tables_total", {})
    pre_it = pre.get("indexes_total", {})
    post_it = post.get("indexes_total", {})

    approx = False

    if pre_tt or post_tt:
        pre_tb = pre_tt.get("bytes", 0)
        post_tb = post_tt.get("bytes", 0)
        pre_tr = pre_tt.get("rows", 0)
        post_tr = post_tt.get("rows", 0)
        pre_ib = pre_it.get("bytes", 0)
        post_ib = post_it.get("bytes", 0)
    else:
        has_top = (
            pre.get("top_tables") or pre.get("all_tables")
            or post.get("top_tables") or post.get("all_tables")
        )
        if not has_top:
            return None
        approx = True
        pre_tb, pre_tr, pre_ib = _sum_from_top(
            pre, "all_tables" if pre.get("all_tables") else "top_tables",
            "all_indexes" if pre.get("all_indexes") else "top_indexes",
        )
        post_tb, post_tr, post_ib = _sum_from_top(
            post, "all_tables" if post.get("all_tables") else "top_tables",
            "all_indexes" if post.get("all_indexes") else "top_indexes",
        )

    result = {}
    rows = []

    table_delta = post_tb - pre_tb
    result["tables"] = {
        "pre_bytes": pre_tb, "post_bytes": post_tb,
        "delta_bytes": table_delta,
    }
    rows.append(["Table Data", pretty_bytes(pre_tb), pretty_bytes(post_tb), color_delta(table_delta), color_pct(pre_tb, post_tb)])

    row_delta = post_tr - pre_tr
    result["rows"] = {
        "pre": pre_tr, "post": post_tr, "delta": row_delta,
    }
    sign = "+" if row_delta >= 0 else ""
    rows.append(["Rows", f"{pre_tr:,}", f"{post_tr:,}", f"{sign}{row_delta:,}", ""])

    idx_delta = post_ib - pre_ib
    result["indexes"] = {
        "pre_bytes": pre_ib, "post_bytes": post_ib,
        "delta_bytes": idx_delta,
    }
    rows.append(["Index Data", pretty_bytes(pre_ib), pretty_bytes(post_ib), color_delta(idx_delta), color_pct(pre_ib, post_ib)])

    combined_pre = pre_tb + pre_ib
    combined_post = post_tb + post_ib
    combined_delta = combined_post - combined_pre
    result["combined"] = {
        "pre_bytes": combined_pre, "post_bytes": combined_post,
        "delta_bytes": combined_delta,
    }
    rows.append(["Combined", pretty_bytes(combined_pre), pretty_bytes(combined_post), color_delta(combined_delta), color_pct(combined_pre, combined_post)])

    label = "Totals Summary (top-N only, approximate)" if approx else "Totals Summary"
    _section(label)
    print_table(["", "Pre", "Post", "Delta", "Change"], rows)

    if approx:
        result["approximate"] = True
    return result


def diff_bloat(pre: dict, post: dict) -> Optional[dict]:
    pre_bloat = {f"{b['schema']}.{b['table']}": b for b in pre.get("table_bloat", [])}
    post_bloat = {f"{b['schema']}.{b['table']}": b for b in post.get("table_bloat", [])}

    if not pre_bloat and not post_bloat:
        return None

    all_keys = sorted(set(pre_bloat) | set(post_bloat))
    rows = []
    json_rows = []

    for key in all_keys:
        pb = pre_bloat.get(key)
        pob = post_bloat.get(key)

        pre_dead = pb["dead_tuples"] if pb else 0
        post_dead = pob["dead_tuples"] if pob else 0
        dead_delta = post_dead - pre_dead

        pre_pct = pb["bloat_pct"] if pb else 0.0
        post_pct = pob["bloat_pct"] if pob else 0.0

        sign = "+" if dead_delta >= 0 else ""
        dead_delta_str = f"{sign}{dead_delta:,}"
        if dead_delta > 0:
            dead_delta_str = _c(Colors.RED, dead_delta_str)
        elif dead_delta < 0:
            dead_delta_str = _c(Colors.GREEN, dead_delta_str)

        rows.append((abs(dead_delta), [
            key,
            f"{pre_dead:,}" if pb else "-",
            f"{post_dead:,}" if pob else "-",
            dead_delta_str,
            f"{pre_pct:.1f}%" if pb else "-",
            f"{post_pct:.1f}%" if pob else "-",
        ]))
        json_rows.append((abs(dead_delta), {
            "name": key,
            "pre_dead": pre_dead, "post_dead": post_dead,
            "delta_dead": dead_delta,
            "pre_bloat_pct": pre_pct, "post_bloat_pct": post_pct,
        }))

    rows.sort(key=lambda x: x[0], reverse=True)
    json_rows.sort(key=lambda x: x[0], reverse=True)

    _section("Table Bloat Changes")
    print_table(
        ["Table", "Pre Dead", "Post Dead", "Delta", "Pre Bloat %", "Post Bloat %"],
        [r[1] for r in rows],
    )

    return {"tables": [r[1] for r in json_rows]}


# ── main ─────────────────────────────────────────────────────────────


def main():
    parser = argparse.ArgumentParser(
        description="Compare two DB state JSON snapshots from show_xray_db_config.py",
    )
    parser.add_argument(
        "pre_json",
        help="Path to the pre-run JSON file",
    )
    parser.add_argument(
        "post_json",
        help="Path to the post-run JSON file",
    )
    parser.add_argument(
        "--output", "-o",
        metavar="FILE",
        help="Write comparison results to a JSON file",
    )
    args = parser.parse_args()

    pre = load_json(args.pre_json)
    post = load_json(args.post_json)

    print(_c(Colors.BOLD_BLUE, "XRAY PostgreSQL DB State Comparison"))
    print_kv([
        ("Pre:", args.pre_json),
        ("Post:", args.post_json),
    ])

    json_out = {"pre_file": args.pre_json, "post_file": args.post_json}
    any_output = False

    result = diff_database_size(pre, post)
    if result:
        json_out["database_size"] = result
        any_output = True

    result = diff_tablespace(pre, post)
    if result:
        json_out["tablespace"] = result
        any_output = True

    result = diff_tables(pre, post)
    if result:
        json_out["table_changes"] = result
        any_output = True

    result = diff_indexes(pre, post)
    if result:
        json_out["index_changes"] = result
        any_output = True

    result = diff_totals(pre, post)
    if result:
        json_out["totals"] = result
        any_output = True

    result = diff_bloat(pre, post)
    if result:
        json_out["bloat"] = result
        any_output = True

    if not any_output:
        print(f"\n  {_c(Colors.MAGENTA, 'No comparable data found in the JSON files.')}")
        print(f"  Hint: run show_xray_db_config.py with --size --output to capture size data.")

    if args.output:
        with open(args.output, "w") as f:
            json.dump(json_out, f, indent=2)
        print(f"\n{_c(Colors.GREEN, f'  ✓ JSON diff written to {args.output}')}")

    print(f"\n{_c(Colors.GREEN, '  ✓ Done')}")


if __name__ == "__main__":
    main()
