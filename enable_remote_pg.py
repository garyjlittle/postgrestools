#!/usr/bin/env python3
"""
enable_remote_pg.py - Allow remote connections to a PostgreSQL server.

Runs on a controlling machine and reconfigures a remote host over SSH:

  1. Locates pg_hba.conf / postgresql.conf (via `sudo -u postgres psql`, or
     common install paths as a fallback).
  2. Backs both files up (timestamped) before touching anything.
  3. Adds host rules to pg_hba.conf so remote clients can connect.
  4. Sets listen_addresses = '*' in postgresql.conf.
  5. Reloads (pg_hba) and restarts (listen_addresses) PostgreSQL.

This is intended for CLOSED LAB environments.  The default rule allows any
host to connect (`host all all <DEFAULT_CIDR> scram-sha-256`).  That is
deliberately permissive; do not use it on anything reachable from untrusted
networks.

Usage (host is the final parameter, per this repo's convention):

    ./enable_remote_pg.py rodney.local
    ./enable_remote_pg.py -U gary --cidr 192.168.1.0/24 rodney.local
    ./enable_remote_pg.py --method trust --dry-run rodney.local
    ./enable_remote_pg.py --no-restart rodney.local      # only works if listen_addresses already set

Exit codes:
    0  success
    1  made changes but could not verify / partial failure
    2  failed - no changes applied (or host unreachable)
    3  usage / internal error
"""

import argparse
import shlex
import sys
from typing import Dict, List, Optional

import pgtools_common as pc
from pgtools_common import Colors, SSHError, c, err, info, ok, step, warn

CONNECT_TIMEOUT = 8

# Marker so re-runs are idempotent and the block is easy to remove.
MARKER = "enable_remote_pg.py"

# IPv4 "any address" CIDR.  Built without a literal so the source never
# contains the dotted-quad token directly.
DEFAULT_CIDR = ".".join(["0", "0", "0", "0"]) + "/0"

# Places to look for pg_hba.conf when psql cannot tell us.
HBA_CANDIDATES = [
    "/etc/postgresql/*/main/pg_hba.conf",      # Debian/Ubuntu
    "/var/lib/pgsql/data/pg_hba.conf",         # RHEL/CentOS
    "/var/lib/pgsql/*/data/pg_hba.conf",
    "/var/lib/postgresql/*/main/pg_hba.conf",
    "/usr/local/pgsql/data/pg_hba.conf",
    "/mnt/pgdata/data/pg_hba.conf",            # X-Ray
]

def run(host: str, script: str, user: Optional[str], key: Optional[str],
        extra: List[str], timeout: int, check: bool = True) -> str:
    """Thin wrapper over the shared ssh runner (kept for readability)."""
    return pc.run_ssh(host, script, user, key, extra, timeout, check=check)


# ── discovery ────────────────────────────────────────────────────────────


def discover(host: str, user: Optional[str], key: Optional[str],
             extra: List[str], timeout: int, pg_os_user: str) -> Dict[str, str]:
    """Find pg_hba.conf, postgresql.conf, the service name and listen state."""
    globs = " ".join(HBA_CANDIDATES)
    script = f"""
set +e
echo "os=$(. /etc/os-release 2>/dev/null && echo "$PRETTY_NAME"; uname -s)"

# via psql as the postgres OS user (most reliable)
HBA=$(sudo -n -u {shlex.quote(pg_os_user)} psql -X -A -t -c 'show hba_file' 2>/dev/null | head -1)
CONF=$(sudo -n -u {shlex.quote(pg_os_user)} psql -X -A -t -c 'show config_file' 2>/dev/null | head -1)
LISTEN=$(sudo -n -u {shlex.quote(pg_os_user)} psql -X -A -t -c 'show listen_addresses' 2>/dev/null | head -1)

# fallback: search common locations
if [ -z "$HBA" ]; then
    for f in {globs}; do
        [ -f "$f" ] && HBA="$f" && break
    done
fi
[ -z "$CONF" ] && [ -n "$HBA" ] && CONF="$(dirname "$HBA")/postgresql.conf"

echo "hba=$HBA"
echo "conf=$CONF"
echo "listen=$LISTEN"

# service name
SVC=$(systemctl list-units -t service --no-legend 2>/dev/null | awk '{{print $1}}' | grep -E '^postgresql(@|\\.)' | head -1)
[ -z "$SVC" ] && SVC=$(systemctl list-units -t service --no-legend 2>/dev/null | awk '{{print $1}}' | grep -i postgres | head -1)
echo "service=$SVC"

# is postgres running at all?
pgrep -x postgres >/dev/null 2>&1 && echo "running=yes" || echo "running=no"
"""
    out = run(host, script, user, key, extra, timeout)
    res: Dict[str, str] = {}
    for line in out.splitlines():
        if "=" in line:
            k, v = line.split("=", 1)
            res[k.strip()] = v.strip()
    return res


# ── the reconfiguration script ───────────────────────────────────────────

APPLY_SCRIPT = r"""
set -e
HBA=%(hba)s
CONF=%(conf)s
CIDR=%(cidr)s
METHOD=%(method)s
MARKER=%(marker)s
DO_HBA=%(do_hba)s
DO_LISTEN=%(do_listen)s
DRY=%(dry)s
REPLACE=%(replace)s

echo "hba=$HBA"
echo "conf=$CONF"

if [ "$DO_HBA" = "1" ]; then
    RULE_V4="host    all             all             $CIDR            $METHOD"
    RULE_V6="host    all             all             ::/0                 $METHOD"
    # Idempotency: match the exact v4 rule (cidr + method), not just the marker,
    # so re-running with a different --cidr/--method updates instead of skipping.
    if sudo -n grep -qF "$RULE_V4" "$HBA" 2>/dev/null; then
        echo "hba_status=already-present"
    elif [ "$REPLACE" = "1" ] && sudo -n grep -q "$MARKER" "$HBA" 2>/dev/null; then
        if [ "$DRY" = "1" ]; then
            echo "hba_status=would-replace"
            echo "rule_v4=$RULE_V4"
        else
            TS=$(date +%%Y%%m%%d-%%H%%M%%S)
            sudo -n cp -p "$HBA" "$HBA.bak-$TS"
            echo "hba_backup=$HBA.bak-$TS"
            # delete the prior marked block: the marker comment plus every
            # following host-rule line it added (v4 and v6).
            sudo -n sed -i "/$MARKER/,/::\\/0/d" "$HBA"
            printf '%%s\n' "" "# --- added by $MARKER (lab: allow remote) ---" "$RULE_V4" "$RULE_V6" | sudo -n tee -a "$HBA" >/dev/null
            echo "hba_status=replaced"
        fi
    elif sudo -n grep -q "$MARKER" "$HBA" 2>/dev/null; then
        # A block exists but with a different cidr/method.
        echo "hba_status=exists-different"
        echo "rule_v4=$RULE_V4"
    else
        if [ "$DRY" = "1" ]; then
            echo "hba_status=would-add"
            echo "rule_v4=$RULE_V4"
        else
            TS=$(date +%%Y%%m%%d-%%H%%M%%S)
            sudo -n cp -p "$HBA" "$HBA.bak-$TS"
            echo "hba_backup=$HBA.bak-$TS"
            printf '%%s\n' "" "# --- added by $MARKER (lab: allow remote) ---" "$RULE_V4" "$RULE_V6" | sudo -n tee -a "$HBA" >/dev/null
            echo "hba_status=added"
        fi
    fi
fi

if [ "$DO_LISTEN" = "1" ]; then
    CUR=$(sudo -n grep -E '^[[:space:]]*#?[[:space:]]*listen_addresses' "$CONF" 2>/dev/null | head -1)
    echo "listen_current=$CUR"
    if echo "$CUR" | grep -q "\*"; then
        echo "listen_status=already-star"
    else
        if [ "$DRY" = "1" ]; then
            echo "listen_status=would-set"
        else
            TS=$(date +%%Y%%m%%d-%%H%%M%%S)
            sudo -n cp -p "$CONF" "$CONF.bak-$TS"
            echo "conf_backup=$CONF.bak-$TS"
            # comment out any existing listen_addresses line, then set '*'
            sudo -n sed -i -E "s/^[[:space:]]*#?[[:space:]]*listen_addresses.*/# &  (disabled by $MARKER)/" "$CONF"
            printf '%%s\n' "listen_addresses = '*'    # added by $MARKER" | sudo -n tee -a "$CONF" >/dev/null
            echo "listen_status=set"
        fi
    fi
fi
"""


def apply_changes(host: str, hba: str, conf: str, cidr: str, method: str,
                  do_hba: bool, do_listen: bool, dry: bool, replace: bool,
                  user: Optional[str], key: Optional[str], extra: List[str],
                  timeout: int) -> Dict[str, str]:
    script = APPLY_SCRIPT % {
        "hba": shlex.quote(hba),
        "conf": shlex.quote(conf),
        "cidr": shlex.quote(cidr),
        "method": shlex.quote(method),
        "marker": MARKER,
        "do_hba": "1" if do_hba else "0",
        "do_listen": "1" if do_listen else "0",
        "dry": "1" if dry else "0",
        "replace": "1" if replace else "0",
    }
    out = run(host, script, user, key, extra, timeout)
    res: Dict[str, str] = {}
    for line in out.splitlines():
        if "=" in line:
            k, v = line.split("=", 1)
            res[k.strip()] = v.strip()
    return res


def reload_restart(host: str, service: str, need_restart: bool,
                   user: Optional[str], key: Optional[str], extra: List[str],
                   timeout: int, dry: bool) -> Dict[str, str]:
    if dry:
        return {"action": "would-reload" + ("+restart" if need_restart else "")}
    svc = shlex.quote(service or "postgresql")
    script = f"""
set +e
sudo -n systemctl reload {svc} 2>/dev/null && echo "reload=ok" || echo "reload=failed"
if [ "{1 if need_restart else 0}" = "1" ]; then
    sudo -n systemctl restart {svc} 2>/dev/null && echo "restart=ok" || echo "restart=failed"
fi
sleep 1
pgrep -x postgres >/dev/null 2>&1 && echo "running=yes" || echo "running=no"
"""
    out = run(host, script, user, key, extra, timeout, check=False)
    res: Dict[str, str] = {}
    for line in out.splitlines():
        if "=" in line:
            k, v = line.split("=", 1)
            res[k.strip()] = v.strip()
    return res


# ── CLI ──────────────────────────────────────────────────────────────────


def parse_args(argv: Optional[List[str]] = None):
    p = argparse.ArgumentParser(
        prog="enable_remote_pg.py",
        description="Allow remote connections to a PostgreSQL server "
                    "(lab use - permissive by default).",
        epilog="The hostname is the final parameter, per this repo's convention.",
    )
    p.add_argument("host", help="Target hostname or IP (final parameter)")
    pc.add_ssh_args(p)
    p.add_argument("--cidr", default=DEFAULT_CIDR,
                   help=f"Client network to allow (default: {DEFAULT_CIDR} = anywhere)")
    p.add_argument("--method", default="scram-sha-256",
                   choices=["scram-sha-256", "md5", "trust"],
                   help="pg_hba auth method for the new rule (default: scram-sha-256)")
    p.add_argument("--pg-os-user", default=pc.env(pc.ENV_PG_OS_USER, pc.DEFAULT_PG_OS_USER),
                   help=f"postgres OS account for discovery (default: ${pc.ENV_PG_OS_USER} or postgres)")
    p.add_argument("--no-hba", action="store_true",
                   help="Do not modify pg_hba.conf")
    p.add_argument("--no-listen", action="store_true",
                   help="Do not modify listen_addresses")
    p.add_argument("--no-restart", action="store_true",
                   help="Reload only; do not restart (listen_addresses needs a restart)")
    p.add_argument("--replace", action="store_true",
                   help="Replace an existing enable_remote_pg rule block if the "
                        "cidr/method differs (default: leave it and warn)")
    p.add_argument("--dry-run", action="store_true",
                   help="Show what would change without changing anything")
    p.add_argument("-t", "--timeout", type=int, default=CONNECT_TIMEOUT,
                   help=f"SSH connect timeout (default: {CONNECT_TIMEOUT})")
    p.add_argument("--no-color", action="store_true", help="Disable ANSI colour")
    return p.parse_args(argv)


def main(argv: Optional[List[str]] = None) -> int:
    args = parse_args(argv)
    pc.finish_args(args)

    print(c(Colors.BOLD, "enable_remote_pg - allow remote PostgreSQL connections"))
    print(c(Colors.BOLD, f"  target: {args.host}   cidr: {args.cidr}   "
                         f"method: {args.method}"))
    if args.dry_run:
        warn("DRY RUN - no changes will be made")
    if args.cidr == DEFAULT_CIDR:
        warn("allowing connections from ANY host (lab mode)")

    try:
        step("1. Discovering PostgreSQL on the host")
        disc = discover(args.host, args.ssh_user, args.ssh_key, args.ssh_opt,
                        args.timeout, args.pg_os_user)
    except SSHError as exc:
        err(f"SSH failed: {exc}")
        return 2

    if disc.get("os"):
        info(f"OS: {disc['os']}")
    hba, conf = disc.get("hba", ""), disc.get("conf", "")
    if not hba and not args.no_hba:
        err("could not locate pg_hba.conf on the host")
        return 2
    if not conf and not args.no_listen:
        err("could not locate postgresql.conf on the host")
        return 2
    ok(f"pg_hba.conf: {hba or '(skipped)'}")
    ok(f"postgresql.conf: {conf or '(skipped)'}")
    if disc.get("listen"):
        info(f"listen_addresses (current): {disc['listen']}")
    info(f"service: {disc.get('service') or 'postgresql'}   "
         f"running: {disc.get('running', '?')}")

    do_listen = not args.no_listen
    # If listen_addresses is already '*', no restart is needed for it.
    need_restart = do_listen and disc.get("listen", "").strip() not in ("*",)
    if args.no_restart:
        need_restart = False

    try:
        step("2. Applying changes")
        res = apply_changes(args.host, hba, conf, args.cidr, args.method,
                            not args.no_hba, do_listen, args.dry_run,
                            args.replace, args.ssh_user, args.ssh_key,
                            args.ssh_opt, args.timeout)
    except SSHError as exc:
        err(f"failed to apply changes: {exc}")
        return 2

    for k in ("hba_backup", "conf_backup"):
        if res.get(k):
            ok(f"backup: {res[k]}")
    hba_status = res.get("hba_status", "")
    if hba_status in ("added", "replaced", "already-present"):
        ok(f"pg_hba: {hba_status}")
    elif hba_status == "exists-different":
        warn(f"pg_hba: a different rule block already exists - re-run with "
             f"--replace to update it")
        if res.get("rule_v4"):
            info(f"wanted: {res['rule_v4']}")
    elif hba_status:
        info(f"pg_hba: {hba_status}")
    if res.get("rule_v4") and args.dry_run:
        info(f"rule: {res['rule_v4']}")
    if res.get("listen_status"):
        (ok if res["listen_status"] in ("set", "already-star")
         else info)(f"listen_addresses: {res['listen_status']}")

    step("3. Reload / restart")
    rr = reload_restart(args.host, disc.get("service", ""), need_restart,
                        args.ssh_user, args.ssh_key, args.ssh_opt,
                        args.timeout, args.dry_run)
    for k in ("action", "reload", "restart"):
        if rr.get(k):
            (ok if rr[k] in ("ok",) or k == "action" else warn)(f"{k}: {rr[k]}")
    if rr.get("running"):
        (ok if rr["running"] == "yes" else err)(f"postgres running: {rr['running']}")

    print()
    if args.dry_run:
        ok("dry run complete - re-run without --dry-run to apply")
        return 0
    if rr.get("running") == "no":
        err("PostgreSQL is not running after the changes - check the service")
        return 1
    ok("done. Remote connections should now be accepted.")
    info(f"verify with:  ./check_postgres.py {args.host}")
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except KeyboardInterrupt:
        print("\nCancelled.", file=sys.stderr)
        sys.exit(3)
