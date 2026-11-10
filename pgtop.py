#!/usr/bin/env python3
"""
pgtop.py - a `top`-like live monitor for PostgreSQL processes on a host.

Runs on a controlling machine and samples a remote (or local) host over SSH
once per interval.  For every postgres process it shows:

    PID, per-NUMA-node memory, user%, sys%, idle%, and a memory breakdown
    (the three largest segment types: anonymous / file-backed / shared-mem).

A header summarises the instance (process count, total RSS, total CPU).

The display refreshes every second by default; change it with -i/--interval.

Data sources on the host (all under /proc, read via sudo when needed):
    /proc/<pid>/stat          utime/stime (CPU %)
    /proc/<pid>/status        VmRSS, RssAnon, RssFile, RssShmem
    /proc/<pid>/numa_maps     per-NUMA-node page allocation   (needs root)
    /proc/<pid>/smaps_rollup  Pss_Anon/Pss_File/Pss_Shmem     (needs root)

Usage (host is the final parameter, per this repo's convention):

    ./pgtop.py rodney.local
    ./pgtop.py -U gary -d 2 rodney.local        # refresh every 2 seconds
    ./pgtop.py --no-sudo rodney.local           # skip NUMA/Pss (no root needed)
    ./pgtop.py -n 5 rodney.local                # 5 iterations then exit
    ./pgtop.py --once --json rodney.local       # single sample as JSON

Press Ctrl-C to stop.
"""

import argparse
import json
import os
import re
import sys
import time
from typing import Dict, List, Optional

import pgtools_common as pc
from pgtools_common import Colors, SSHError, c

# ── remote collector script ──────────────────────────────────────────────
# Emits machine-parseable lines:
#   meta.nproc / meta.clk / meta.nodes / meta.sudo
#   proc.<pid>.field=value   (stat, status, numa, smaps)
COLLECT_SCRIPT = r"""
set +e
SUDO=""
if [ "{use_sudo}" = "1" ] && sudo -n true 2>/dev/null; then
    SUDO="sudo -n"
    echo "meta.sudo=1"
else
    echo "meta.sudo=0"
fi
echo "meta.nproc=$(nproc 2>/dev/null || echo 1)"
echo "meta.clk=$(getconf CLK_TCK 2>/dev/null || echo 100)"
echo "meta.nodes=$(ls -d /sys/devices/system/node/node* 2>/dev/null | wc -l)"

for p in $(pgrep -x postgres 2>/dev/null); do
    [ -d "/proc/$p" ] || continue
    # command / role
    cmd=$(tr '\0' ' ' < /proc/$p/cmdline 2>/dev/null)
    echo "proc.$p.cmd=$cmd"
    # cpu times (fields 14,15 of /proc/stat)
    awk '{{print "proc." P ".utime=" $14; print "proc." P ".stime=" $15}}' P="$p" /proc/$p/stat 2>/dev/null
    # memory from status
    awk '/^(VmRSS|RssAnon|RssFile|RssShmem):/{{gsub(/:/,"",$1); print "proc." P "." $1 "=" $2}}' P="$p" /proc/$p/status 2>/dev/null
    # per-NUMA-node pages from numa_maps (needs root)
    if [ -n "$SUDO" ]; then
        $SUDO awk '{{for(i=1;i<=NF;i++){{if($i ~ /^N[0-9]+=/){{split($i,a,"="); n[a[1]]+=a[2]}}}}}} END{{for(k in n) print "proc." P ".numa." k "=" n[k]*4}}' P="$p" /proc/$p/numa_maps 2>/dev/null
        # pss breakdown from smaps_rollup (needs root)
        $SUDO awk '/^(Pss_Anon|Pss_File|Pss_Shmem):/{{gsub(/:/,"",$1); print "proc." P "." $1 "=" $2}}' P="$p" /proc/$p/smaps_rollup 2>/dev/null
    fi
done
"""


def collect(host: str, use_sudo: bool, user: Optional[str], key: Optional[str],
            extra: List[str], timeout: int) -> Dict:
    """Run the collector on the host and parse the key=value output."""
    is_local = host in ("localhost", "[IP_ADDRESS]", "::1")
    script = COLLECT_SCRIPT.format(use_sudo="1" if use_sudo else "0")
    import subprocess
    if is_local:
        proc = subprocess.run(["sh", "-c", script], capture_output=True,
                              text=True, timeout=timeout + 20)
        out = proc.stdout
    else:
        out = pc.run_ssh(host, script, user, key, extra, timeout, check=False)
    return _parse(out)


def _parse(text: str) -> Dict:
    meta: Dict[str, str] = {}
    procs: Dict[int, Dict[str, object]] = {}
    for line in text.splitlines():
        line = line.strip()
        if "=" not in line:
            continue
        key, val = line.split("=", 1)
        if key.startswith("meta."):
            meta[key[5:]] = val
            continue
        m = re.match(r"proc\.(\d+)\.(.+)", key)
        if not m:
            continue
        pid = int(m.group(1))
        field = m.group(2)
        entry = procs.setdefault(pid, {})
        if field.startswith("numa."):
            node = field[5:]
            numa = entry.setdefault("numa", {})
            try:
                numa[node] = int(val)
            except ValueError:
                pass
        elif field == "cmd":
            entry["cmd"] = val
        else:
            try:
                entry[field] = int(val)
            except ValueError:
                entry[field] = val
    return {"meta": meta, "procs": procs}


# ── role / display helpers ───────────────────────────────────────────────

_ROLES = [
    ("checkpointer", "checkpointer"),
    ("background writer", "bgwriter"),
    ("walwriter", "walwriter"),
    ("autovacuum launcher", "av launcher"),
    ("autovacuum worker", "av worker"),
    ("logical replication launcher", "logical launcher"),
    ("logical replication worker", "logical worker"),
    ("walsender", "walsender"),
    ("walreceiver", "walreceiver"),
    ("stats collector", "stats"),
    ("archiver", "archiver"),
    ("pgarch", "archiver"),
    ("postgres:", "backend"),
]


def role_of(cmd: str, is_postmaster: bool) -> str:
    if is_postmaster:
        return "postmaster"
    low = cmd.lower()
    for pat, name in _ROLES:
        if pat in low:
            return name
    return "postgres"


def _fmt_kb(kb: float) -> str:
    if kb >= 1024 * 1024:
        return f"{kb / (1024 * 1024):.1f}G"
    if kb >= 1024:
        return f"{kb / 1024:.0f}M"
    return f"{int(kb)}K"


# ── sampling / rendering ─────────────────────────────────────────────────


def sample(host: str, args) -> Dict:
    data = collect(host, args.use_sudo, args.ssh_user, args.ssh_key,
                   args.ssh_opt, args.timeout)
    data["ts"] = time.time()
    return data


def compute_rows(prev: Optional[Dict], cur: Dict, interval: float) -> List[Dict]:
    """Build display rows with CPU% computed against the previous sample.

    CPU% is measured against the *actual* wall-clock time between the two
    samples (which includes the SSH/collection overhead), not the configured
    interval - otherwise a slow collection inflates the percentages.
    """
    meta = cur["meta"]
    clk = max(1, int(meta.get("clk", 100)))
    prev_procs = prev["procs"] if prev else {}
    # real elapsed seconds between the two samples
    elapsed = (cur.get("ts", 0) - prev.get("ts", 0)) if prev else interval
    if elapsed <= 0:
        elapsed = interval

    rows = []
    for pid, p in cur["procs"].items():
        ut = int(p.get("utime", 0))
        st = int(p.get("stime", 0))
        user_pct = sys_pct = 0.0
        if prev and pid in prev_procs:
            pp = prev_procs[pid]
            du = ut - int(pp.get("utime", 0))
            ds = st - int(pp.get("stime", 0))
            # jiffies -> seconds of cpu used over the real elapsed time.
            # 100% == one fully-used CPU (same convention as top).
            user_pct = 100.0 * (du / clk) / elapsed
            sys_pct = 100.0 * (ds / clk) / elapsed
        idle_pct = max(0.0, 100.0 - user_pct - sys_pct)

        rss = int(p.get("VmRSS", 0))
        anon = int(p.get("RssAnon", 0))
        filem = int(p.get("RssFile", 0))
        shmem = int(p.get("RssShmem", 0))
        # top-3 memory segment types for this pid
        segs = sorted((("anon", anon), ("file", filem), ("shmem", shmem)),
                      key=lambda x: x[1], reverse=True)
        top3 = " ".join(f"{n}:{_fmt_kb(v)}" for n, v in segs[:3] if v > 0)

        numa = p.get("numa", {})
        numa_s = " ".join(f"{node}:{_fmt_kb(kb)}" for node, kb in sorted(numa.items())) or "-"

        rows.append({
            "pid": pid,
            "role": role_of(str(p.get("cmd", "")), is_postmaster=(pid == _postmaster_pid(cur))),
            "user": user_pct,
            "sys": sys_pct,
            "idle": idle_pct,
            "rss": rss,
            "numa": numa_s,
            "top3": top3,
        })
    rows.sort(key=lambda r: r["rss"], reverse=True)
    return rows


def _postmaster_pid(cur: Dict) -> int:
    """The postmaster is the process whose cmdline starts with the binary path."""
    for pid, p in cur["procs"].items():
        cmd = str(p.get("cmd", ""))
        if cmd.startswith("/") and "postgres" in cmd and "-D" in cmd:
            return pid
    return min(cur["procs"], default=0)


def render(host: str, cur: Dict, rows: List[Dict], interval: float,
           first: bool) -> str:
    meta = cur["meta"]
    nproc = meta.get("nproc", "?")
    nodes = meta.get("nodes", "?")
    sudo = meta.get("sudo", "0")
    total_rss = sum(r["rss"] for r in rows)
    tot_user = sum(r["user"] for r in rows)
    tot_sys = sum(r["sys"] for r in rows)

    lines = []
    lines.append(c(Colors.BOLD,
        f"pgtop - {host}  |  {time.strftime('%H:%M:%S')}  |  "
        f"every {interval:g}s  |  procs {len(rows)}  cpus {nproc}  numa {nodes}"
        + ("" if sudo == "1" else "  (no sudo: NUMA/Pss limited)")))
    lines.append(c(Colors.CYAN,
        f"  total RSS {_fmt_kb(total_rss)}   CPU user {tot_user:.1f}%  "
        f"sys {tot_sys:.1f}%"))
    header = (f"  {'PID':>7} {'ROLE':<16} {'USER%':>6} {'SYS%':>6} {'IDLE%':>6} "
              f"{'RSS':>8}  {'NUMA':<18} TOP MEM SEGMENTS")
    lines.append(c(Colors.BOLD, header))
    for r in rows:
        lines.append(
            f"  {r['pid']:>7} {r['role']:<16} {r['user']:>6.1f} {r['sys']:>6.1f} "
            f"{r['idle']:>6.1f} {_fmt_kb(r['rss']):>8}  {r['numa']:<18} {r['top3']}")
    if first:
        lines.append(c(Colors.DIM if hasattr(Colors, "DIM") else Colors.CYAN,
                       "  (CPU% shown from the 2nd sample onward)"))
    return "\n".join(lines)


# ── CLI ──────────────────────────────────────────────────────────────────


def parse_args(argv: Optional[List[str]] = None):
    p = argparse.ArgumentParser(
        prog="pgtop.py",
        description="A top-like live monitor for PostgreSQL processes.",
        epilog="The hostname is the final parameter, per this repo's convention.",
    )
    p.add_argument("host", help="Target hostname or IP (final parameter)")
    pc.add_ssh_args(p)
    p.add_argument("-d", "--interval", type=float, default=1.0,
                   help="Seconds between updates (default: 1)")
    p.add_argument("-n", "--iterations", type=int, default=0,
                   help="Number of samples before exiting (default: run forever)")
    p.add_argument("--once", action="store_true",
                   help="Take a single sample and exit")
    p.add_argument("--no-sudo", dest="use_sudo", action="store_false",
                   help="Do not use sudo (skips NUMA and Pss breakdown)")
    p.add_argument("--json", action="store_true",
                   help="Emit JSON (implies --once unless -n is given)")
    p.add_argument("--no-color", action="store_true", help="Disable ANSI colour")
    p.add_argument("-t", "--timeout", type=int, default=8,
                   help="SSH connect timeout (default: 8)")
    p.set_defaults(use_sudo=True)
    return p.parse_args(argv)


def main(argv: Optional[List[str]] = None) -> int:
    args = parse_args(argv)
    if args.no_color or args.json:
        Colors.disable()
    interval = max(0.2, args.interval)
    iterations = 1 if (args.once or args.json) and args.iterations == 0 \
        else args.iterations

    prev: Optional[Dict] = None
    count = 0
    try:
        while True:
            cur = sample(args.host, args)
            rows = compute_rows(prev, cur, interval)
            if args.json:
                print(json.dumps({
                    "host": args.host, "ts": time.time(),
                    "interval": interval, "meta": cur["meta"],
                    "processes": rows,
                }, indent=2))
            else:
                if not args.once and count > 0:
                    # clear screen + move home for a top-like refresh
                    sys.stdout.write("\033[2J\033[H")
                sys.stdout.write(render(args.host, cur, rows, interval,
                                        first=(prev is None)) + "\n")
                sys.stdout.flush()
            count += 1
            if iterations and count >= iterations:
                break
            prev = cur
            time.sleep(interval)
    except SSHError as exc:
        pc.err(f"SSH failed: {exc}")
        return 2
    except KeyboardInterrupt:
        if not args.json:
            print()
        return 0
    return 0


if __name__ == "__main__":
    sys.exit(main())
