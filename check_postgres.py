#!/usr/bin/env python3
"""
check_postgres.py - Report whether PostgreSQL is installed and/or running on a host.

Runs on a controlling machine (macOS laptop or Linux jump box) and inspects a
remote host.  Two independent probes are used, so a partial picture is still
reported if only one of them is available:

  1. Network probe  - TCP connect to the PostgreSQL port and speak just enough
                      of the v3 wire protocol (SSLRequest + StartupMessage) to
                      prove a real PostgreSQL postmaster is answering.
                      No credentials, no third-party modules required.
  2. SSH probe      - Look for installed binaries/packages, the systemd/SysV
                      service state and running postgres processes.

When the server answers, the tool also lists the databases in the instance.
With --pg-password (or $PGPASSWORD) it authenticates over the wire protocol
(trust / cleartext / md5 / scram-sha-256) and queries pg_database directly -
still no third-party modules.  Without credentials it falls back to a local
psql if one is installed.

Usage (host is the final parameter, per this repo's convention):

    ./check_postgres.py perf105-xray-postgres-server
    ./check_postgres.py -p 5432 -U nutanix -i ~/.ssh/id_rsa myhost
    ./check_postgres.py --no-ssh 10.0.0.5
    ./check_postgres.py --pg-user postgres --pg-password secret myhost
    ./check_postgres.py --json myhost | jq .

Exit codes:
    0  PostgreSQL is running (and answering on the port)
    1  Installed but not running / not answering
    2  Not installed, or the host is unreachable
    3  Usage / internal error
"""

import argparse
import errno
import json
import os
import shutil
import socket
import ssl
import subprocess
import sys
from typing import Dict, List, Optional, Tuple

import pgtools_common as pc

# ── constants ────────────────────────────────────────────────────────────

DEFAULT_PORT = 5432
CONNECT_TIMEOUT = 5.0

SSL_REQUEST_CODE = 80877103
PROTOCOL_VERSION_3 = 196608  # 3.0

# Well-known places PostgreSQL binaries live on Linux / macOS installs.
BIN_SEARCH_DIRS = [
    "/usr/bin",
    "/usr/sbin",
    "/usr/local/bin",
    "/usr/local/pgsql/bin",
    "/usr/pgsql-17/bin",
    "/usr/pgsql-16/bin",
    "/usr/pgsql-15/bin",
    "/usr/pgsql-14/bin",
    "/usr/pgsql-13/bin",
    "/usr/lib/postgresql/17/bin",
    "/usr/lib/postgresql/16/bin",
    "/usr/lib/postgresql/15/bin",
    "/usr/lib/postgresql/14/bin",
    "/usr/lib/postgresql/13/bin",
    "/usr/pgsql/bin",
    "/opt/pgsql/bin",
    "/mnt/pgdata/bin",
    "/home/nutanix/bin",
    "/opt/homebrew/opt/postgresql@17/bin",
    "/opt/homebrew/opt/postgresql@16/bin",
    "/opt/homebrew/opt/postgresql@15/bin",
    "/opt/homebrew/bin",
    "/usr/local/Cellar/postgresql/*/bin",
]

PG_COMMANDS = ["postgres", "pg_ctl", "psql", "initdb", "pg_isready", "pg_config"]

# A single remote shell snippet that gathers everything the SSH probe needs.
# Output is parsed as "key=value" lines so it stays easy to extend.
SSH_PROBE_SCRIPT = r"""
set +e
OS_ID=$(. /etc/os-release 2>/dev/null && echo "$PRETTY_NAME")
[ -z "$OS_ID" ] && OS_ID=$(uname -s)$(uname -r)
echo "os=$OS_ID"
echo "kernel=$(uname -r)"
echo "arch=$(uname -m)"
echo "uptime=$(uptime -p 2>/dev/null || uptime)"

# --- binaries on PATH ---
for c in %(cmds)s; do
    p=$(command -v $c 2>/dev/null)
    [ -n "$p" ] && echo "bin.$c=$p"
done

# --- binaries in the usual install locations ---
for d in %(dirs)s; do
    for c in postgres pg_ctl psql initdb; do
        [ -x "$d/$c" ] && echo "found=$d/$c"
    done
done

# --- version strings ---
if command -v postgres >/dev/null 2>&1; then
    echo "version=$(postgres --version 2>&1 | head -1)"
elif command -v pg_config >/dev/null 2>&1; then
    echo "version=$(pg_config --version 2>&1 | head -1)"
fi

# --- installed packages ---
if command -v rpm >/dev/null 2>&1; then
    rpm -qa 2>/dev/null | grep -Ei '^postgres' | sort > /tmp/.pgpkgs.$$ 2>/dev/null
    [ -s /tmp/.pgpkgs.$$ ] && echo "pkgs=$(paste -sd, /tmp/.pgpkgs.$$)" && echo "pkg_mgr=rpm"
    rm -f /tmp/.pgpkgs.$$
elif command -v dpkg-query >/dev/null 2>&1; then
    pkgs=$(dpkg-query -W -f='${Package} ${Version}\n' 2>/dev/null | grep -Ei '^postgres' | paste -sd, -)
    [ -n "$pkgs" ] && echo "pkgs=$pkgs" && echo "pkg_mgr=dpkg"
elif command -v brew >/dev/null 2>&1; then
    pkgs=$(brew list --versions 2>/dev/null | grep -Ei 'postgres|pgvector' | paste -sd, -)
    [ -n "$pkgs" ] && echo "pkgs=$pkgs" && echo "pkg_mgr=brew"
fi

# --- service manager state ---
if command -v systemctl >/dev/null 2>&1; then
    for s in $(systemctl -t service --all --no-legend 2>/dev/null | awk '{print $2}' | grep -Ei 'postgres|pgsql' | sort -u); do
        st=$(systemctl is-active "$s" 2>/dev/null)
        en=$(systemctl is-enabled "$s" 2>/dev/null)
        echo "service=$s:$st:$en"
    done
fi
if command -v service >/dev/null 2>&1; then
    for s in postgresql postgresql-15 postgresql-14; do
        [ -x "/etc/init.d/$s" ] && echo "initd=$s:$(/etc/init.d/$s status >/dev/null 2>&1 && echo active || echo inactive)"
    done
fi

# --- running processes ---
pids=$(pgrep -x postgres 2>/dev/null | tr '\n' ' ')
echo "pgprocs=$(echo $pids | wc -w)"
first=$(echo $pids | awk '{print $1}')
if [ -n "$first" ]; then
    echo "pgpid=$first"
    echo "pguser=$(ps -o user= -p $first 2>/dev/null | tr -d ' ')"
    echo "pgstart=$(ps -o lstart= -p $first 2>/dev/null | sed 's/^ *//')"
    # data directory and port from the postmaster command line
    cl=$(tr '\0' ' ' < /proc/$first/cmdline 2>/dev/null)
    echo "pgcmdline=$cl"
    case "$cl" in
        *"-D "*) datadir=$(echo "$cl" | sed -n 's/.*-D \([^ ]*\).*/\1/p')
                 [ -n "$datadir" ] && echo "datadir=$datadir" ;;
    esac
    case "$cl" in
        *"-p "*) echo "pgport=$(echo "$cl" | sed -n 's/.*-p \([0-9]*\).*/\1/p')" ;;
    esac
fi

# --- listening sockets ---
if command -v ss >/dev/null 2>&1; then
    ss -ltnp 2>/dev/null | grep -E ':(5432|543[0-9])' > /tmp/.pgsock.$$ 2>/dev/null
    [ -s /tmp/.pgsock.$$ ] && echo "listen=$(awk '{print $4}' /tmp/.pgsock.$$ | sed 's/.*://' | sort -un | paste -sd, -)"
    rm -f /tmp/.pgsock.$$
elif command -v netstat >/dev/null 2>&1; then
    l=$(netstat -ltn 2>/dev/null | awk '{print $4}' | sed -n 's/.*[.:]\(54[0-9][0-9]\)$/\1/p' | sort -un | paste -sd, -)
    [ -n "$l" ] && echo "listen=$l"
fi

# --- unix socket directories ---
for d in /var/run/postgresql /tmp /var/lib/pgsql/.s.PGSQL; do
    ls $d/.s.PGSQL.* >/dev/null 2>&1 && echo "unixsock=$d"
done

# --- common data directories ---
for d in /var/lib/pgsql/data /var/lib/postgresql /mnt/pgdata/data /pgdata/data; do
    [ -d "$d" ] && echo "datadir_found=$d"
done

exit 0
"""

# Run as the postgres OS user via sudo.  Uses the unix-socket psql, so it
# works even when the TCP port is closed or firewalled.  %(pguser)s is the
# postgres OS account, %(db)s the maintenance database to connect to.
SUDO_PROBE_SCRIPT = r"""
set +e
PGUSER_OS="%(pguser)s"
DB="%(db)s"

# --- can we sudo at all? ---
if sudo -n true 2>/dev/null; then
    echo "sudo=nopass"
else
    echo "sudo=needpass"
fi

# --- does the target postgres OS user exist? ---
id "$PGUSER_OS" >/dev/null 2>&1 && echo "pgosuser=$PGUSER_OS" || echo "pgosuser="

run_sql() {
    # $1 = SQL.  Prefer the unix socket as the postgres OS user.
    sudo -n -u "$PGUSER_OS" psql -X -A -F '|' -t -d "$DB" -c "$1" 2>/dev/null
}

# If a sudo password was piped in, prime sudo's credential cache with it.
if [ ! -t 0 ]; then
    IFS= read -r SUDOPW 2>/dev/null
    [ -n "$SUDOPW" ] && printf '%%s\n' "$SUDOPW" | sudo -S -p '' true 2>/dev/null
    unset SUDOPW
fi

echo "pg_version=$(run_sql "select version()" | head -1)"
echo "pg_datadir=$(run_sql "show data_directory" | head -1)"
echo "pg_port=$(run_sql "show port" | head -1)"
echo "pg_start=$(run_sql "select pg_postmaster_start_time()" | head -1)"
echo "pg_maxconn=$(run_sql "show max_connections" | head -1)"
echo "pg_dbs=$(run_sql "select string_agg(datname, ',' order by datname) from pg_database where not datistemplate" | head -1)"
echo "pg_roles=$(run_sql "select string_agg(rolname, ',' order by rolname) from pg_roles where rolname !~ '^pg_'" | head -1)"
echo "pg_active=$(run_sql "select count(*) from pg_stat_activity where state = 'active' and pid <> pg_backend_pid()" | head -1)"
echo "pg_dbsize=$(run_sql "select pg_size_pretty(sum(pg_database_size(datname))) from pg_database where not datistemplate" | head -1)"

exit 0
"""


# ── output helpers ───────────────────────────────────────────────────────


class Colors:
    RED = "\033[0;31m"
    GREEN = "\033[0;32m"
    YELLOW = "\033[1;33m"
    BLUE = "\033[0;34m"
    CYAN = "\033[0;36m"
    BOLD = "\033[1m"
    NC = "\033[0m"

    @classmethod
    def disable(cls):
        # Back the shared module's flag too so any pc.* output is also plain.
        pc.Colors.disable()
        for attr in ("RED", "GREEN", "YELLOW", "BLUE", "CYAN", "BOLD", "NC"):
            setattr(cls, attr, "")


def c(color: str, text: str) -> str:
    return f"{color}{text}{Colors.NC}"


def tick(value) -> str:
    if value is True:
        return c(Colors.GREEN, "yes")
    if value is False:
        return c(Colors.RED, "no")
    if value is None:
        return c(Colors.YELLOW, "unknown")
    return str(value)


def hr(char: str = "─", n: int = 64) -> str:
    return char * n


# ── network probe ────────────────────────────────────────────────────────


class NetworkResult:
    def __init__(self, host: str, port: int):
        self.host = host
        self.port = port
        self.resolved: Optional[str] = None
        self.tcp_open: Optional[bool] = None
        self.postgres_protocol: Optional[bool] = None
        self.ssl_supported: Optional[bool] = None
        self.responded: Optional[bool] = None
        self.server_version: Optional[str] = None
        self.detail: str = ""

    def as_dict(self) -> Dict:
        return {
            "host": self.host,
            "resolved_ip": self.resolved,
            "port": self.port,
            "tcp_open": self.tcp_open,
            "postgres_protocol": self.postgres_protocol,
            "ssl_supported": self.ssl_supported,
            "responding": self.responded,
            "server_version": self.server_version,
            "detail": self.detail,
        }


def _recv_exact(sock: socket.socket, n: int, timeout: float) -> bytes:
    sock.settimeout(timeout)
    data = b""
    while len(data) < n:
        chunk = sock.recv(n - len(data))
        if not chunk:
            break
        data += chunk
    return data


def _startup_message(user: str, database: str) -> bytes:
    params = b"user\x00%s\x00database\x00%s\x00\x00" % (
        user.encode(),
        database.encode(),
    )
    body = PROTOCOL_VERSION_3.to_bytes(4, "big") + params
    return (len(body) + 4).to_bytes(4, "big") + body


def _parse_startup_response(payload: bytes) -> Tuple[bool, Optional[str], str]:
    """Return (postgres_responded, server_version, detail)."""
    if not payload:
        return False, None, "connection closed with no protocol response"
    tag = chr(payload[0])
    body = payload[5:]
    if tag == "R":
        # Authentication request - a live postmaster. Try to spot the version
        # in any ParameterStatus that follows in the same read.
        return True, None, "server requested authentication (live postmaster)"
    if tag == "S":
        # ParameterStatus, e.g. server_version
        parts = body.split(b"\x00")
        txt = b" ".join(p.decode(errors="replace") for p in parts if p)
        if b"server_version" in txt:
            maybe = txt.split(b"server_version")[-1].strip().decode(errors="replace")
            return True, maybe or None, f"parameter status: {txt!r}"[:120]
        return True, None, "server sent a parameter status message"
    if tag == "E":
        # ErrorResponse - still proves a PostgreSQL server answered.
        fields = {}
        for part in body.split(b"\x00"):
            if len(part) > 1:
                fields[chr(part[0])] = part[1:].decode(errors="replace")
        sev = fields.get("S", "")
        msg = fields.get("M", "")
        code = fields.get("C", "")
        detail = " ".join(x for x in (sev, code, msg) if x)
        return True, fields.get("v"), f"error response: {detail}".strip()
    return False, None, f"unexpected response byte 0x{payload[0]:02x} - not PostgreSQL?"


def probe_network(host: str, port: int, timeout: float,
                  pg_user: str = "postgres", pg_db: str = "postgres") -> NetworkResult:
    res = NetworkResult(host, port)

    # Resolve first so the report shows what we actually reached.
    try:
        infos = socket.getaddrinfo(host, port, proto=socket.IPPROTO_TCP)
        res.resolved = infos[0][4][0]
    except socket.gaierror as exc:
        res.detail = f"DNS lookup failed: {exc.strerror}"
        return res

    # 1. Plain TCP reachability.  Try every resolved address, preferring the
    #    order the resolver gave us, so dual-stack hosts still work.
    sock = None
    last_err = None
    for family, socktype, proto, _canon, sockaddr in infos:
        if socktype != socket.SOCK_STREAM:
            continue
        addr = sockaddr[0]
        candidate = None
        try:
            candidate = socket.socket(family, socktype, proto)
            candidate.settimeout(timeout)
            candidate.connect(sockaddr)
            sock = candidate
            res.resolved = addr
            break
        except OSError as exc:
            last_err = exc
            if candidate is not None:
                try:
                    candidate.close()
                except OSError:
                    pass
            continue

    if sock is None:
        res.tcp_open = False
        err = last_err or OSError(errno.EHOSTUNREACH, "no usable address")
        res.detail = f"TCP connect to {res.resolved}:{port} failed ({err.strerror})"
        if err.errno == errno.ECONNREFUSED:
            res.detail += " - nothing listening, but the host is up"
        return res
    res.tcp_open = True

    # 2. SSLRequest: PostgreSQL answers this with a single 'S' or 'N' byte.
    #    Any other behaviour means something that is not a postmaster.
    try:
        sock.sendall((8).to_bytes(4, "big") + SSL_REQUEST_CODE.to_bytes(4, "big"))
        reply = _recv_exact(sock, 1, timeout)
        if reply in (b"S", b"N"):
            res.postgres_protocol = True
            res.ssl_supported = reply == b"S"
        else:
            res.postgres_protocol = False
            res.responded = False
            res.detail = (
                "port is open but did not answer the PostgreSQL SSLRequest "
                f"(got {reply[:1]!r}) - a different service may own port {port}"
            )
            sock.close()
            return res
    except OSError as exc:
        res.detail = f"SSLRequest probe failed: {exc.strerror}"
        sock.close()
        return res

    # 3. StartupMessage, wrapped in TLS when the server offers it.
    try:
        if res.ssl_supported:
            ctx = ssl.create_default_context()
            ctx.check_hostname = False
            ctx.verify_mode = ssl.CERT_NONE
            try:
                ssock = ctx.wrap_socket(sock, server_hostname=host)
            except ssl.SSLError as exc:
                res.responded = True
                res.postgres_protocol = True
                res.detail = f"TLS offered but handshake failed: {exc.strerror}"
                return res
        else:
            ssock = sock
        ssock.sendall(_startup_message(pg_user, pg_db))
        payload = _recv_exact(ssock, 40, timeout)
        ok, version, detail = _parse_startup_response(payload)
        res.responded = ok
        res.server_version = version
        res.detail = detail
        try:
            ssock.close()
        except OSError:
            pass
    except OSError as exc:
        res.detail = f"StartupMessage probe failed: {exc.strerror}"
    return res


# ── authenticated wire-protocol query (stdlib only) ──────────────────────

MD5_REQUEST_CODE = 5
CLEARTEXT_REQUEST_CODE = 3
OK_REQUEST_CODE = 0
SASL_REQUEST_CODE = 10
SASL_CONTINUE_CODE = 11
SASL_FINAL_CODE = 12


def _pg_md5_password(user: str, password: str, salt: bytes) -> str:
    import hashlib
    inner = hashlib.md5(password.encode() + user.encode()).hexdigest().encode()
    return "md5" + hashlib.md5(inner + salt).hexdigest()


class _ScramSha256:
    """Minimal SCRAM-SHA-256 client (RFC 5802 / RFC 7677), stdlib only."""

    MECHANISM = "SCRAM-SHA-256"

    def __init__(self, user: str, password: str):
        import base64
        import os as _os
        self._user = user
        self._password = password.encode()
        self._nonce = base64.b64encode(_os.urandom(18)).decode()
        self._client_first_bare = f"n={self._esc(user)},r={self._nonce}"
        self._auth_message = ""
        self._expected_server_sig = b""

    @staticmethod
    def _esc(value: str) -> str:
        return value.replace("=", "=3D").replace(",", "=2C")

    @staticmethod
    def _parse_attrs(data: bytes) -> Dict[str, str]:
        out = {}
        for part in data.decode(errors="replace").split(","):
            if "=" in part:
                k, v = part.split("=", 1)
                out[k] = v
        return out

    def client_first(self) -> bytes:
        # gs2 header "n,," (no channel binding, no authzid) + bare part.
        return ("n,," + self._client_first_bare).encode()

    def client_final(self, server_first: bytes) -> bytes:
        import base64
        import hashlib
        import hmac as _hmac

        attrs = self._parse_attrs(server_first)
        server_nonce = attrs.get("r", "")
        if not server_nonce.startswith(self._nonce):
            raise RuntimeError("SCRAM server nonce does not extend client nonce")
        salt = base64.b64decode(attrs.get("s", ""))
        iterations = int(attrs.get("i", "4096"))

        channel_binding = base64.b64encode(b"n,,").decode()
        client_final_without_proof = f"c={channel_binding},r={server_nonce}"
        self._auth_message = (
            f"{self._client_first_bare},"
            f"{server_first.decode(errors='replace')},"
            f"{client_final_without_proof}"
        )

        salted = hashlib.pbkdf2_hmac("sha256", self._password, salt, iterations)

        def hmac(key: bytes, msg: str) -> bytes:
            return _hmac.new(key, msg.encode(), hashlib.sha256).digest()

        client_key = hmac(salted, "Client Key")
        stored_key = hashlib.sha256(client_key).digest()
        client_signature = hmac(stored_key, self._auth_message)
        proof = bytes(a ^ b for a, b in zip(client_key, client_signature))

        server_key = hmac(salted, "Server Key")
        self._expected_server_sig = hmac(server_key, self._auth_message)

        return (client_final_without_proof
                + ",p=" + base64.b64encode(proof).decode()).encode()

    def verify_server_final(self, server_final: bytes) -> bool:
        import base64
        attrs = self._parse_attrs(server_final)
        sig = attrs.get("v")
        if sig is None:
            raise RuntimeError("SCRAM server-final missing signature")
        return base64.b64decode(sig) == self._expected_server_sig


def _read_message(sock: socket.socket, timeout: float) -> Tuple[bytes, bytes]:
    """Read one backend message; returns (tag_byte, body)."""
    tag = _recv_exact(sock, 1, timeout)
    if len(tag) < 1:
        raise ConnectionError("server closed the connection")
    raw_len = _recv_exact(sock, 4, timeout)
    if len(raw_len) < 4:
        raise ConnectionError("truncated message header")
    length = int.from_bytes(raw_len, "big") - 4
    body = _recv_exact(sock, length, timeout) if length > 0 else b""
    return tag, body


def _error_detail(body: bytes) -> str:
    fields = {}
    for part in body.split(b"\x00"):
        if len(part) > 1:
            fields[chr(part[0])] = part[1:].decode(errors="replace")
    return " ".join(x for x in (fields.get("S"), fields.get("C"), fields.get("M")) if x)


def _authenticate(sock: socket.socket, timeout: float, user: str,
                  password: Optional[str]) -> Tuple[bool, str]:
    """Drive the startup/auth exchange. Returns (ok, detail)."""
    scram: Optional[_ScramSha256] = None
    for _ in range(20):  # bounded: auth exchange is short
        tag, body = _read_message(sock, timeout)
        if tag == b"R":
            code = int.from_bytes(body[:4], "big") if len(body) >= 4 else -1
            if code == OK_REQUEST_CODE:
                continue  # AuthenticationOk; ParameterStatus/ReadyForQuery follow
            if code == CLEARTEXT_REQUEST_CODE:
                if password is None:
                    return False, "server wants a cleartext password - pass --pg-password"
                payload = password.encode() + b"\x00"
                sock.sendall(b"p" + (len(payload) + 4).to_bytes(4, "big") + payload)
                continue
            if code == MD5_REQUEST_CODE:
                if password is None:
                    return False, "server wants an md5 password - pass --pg-password"
                payload = _pg_md5_password(user, password, body[4:8]).encode() + b"\x00"
                sock.sendall(b"p" + (len(payload) + 4).to_bytes(4, "big") + payload)
                continue
            if code == SASL_REQUEST_CODE:
                mechanisms = [m.decode() for m in body[4:].split(b"\x00") if m]
                if password is None:
                    return False, ("server wants SCRAM ("
                                   + "/".join(mechanisms) + ") - pass --pg-password")
                if _ScramSha256.MECHANISM not in mechanisms:
                    return False, ("unsupported SCRAM mechanisms: "
                                   + "/".join(mechanisms))
                scram = _ScramSha256(user, password)
                first = scram.client_first()
                initial = (scram.MECHANISM.encode() + b"\x00"
                           + len(first).to_bytes(4, "big", signed=True) + first)
                sock.sendall(b"p" + (len(initial) + 4).to_bytes(4, "big") + initial)
                continue
            if code == SASL_CONTINUE_CODE:
                if scram is None:
                    return False, "unexpected SASL continuation"
                try:
                    final = scram.client_final(body[4:])
                except RuntimeError as exc:
                    return False, f"SCRAM failed: {exc}"
                sock.sendall(b"p" + (len(final) + 4).to_bytes(4, "big") + final)
                continue
            if code == SASL_FINAL_CODE:
                if scram is None:
                    return False, "unexpected SASL final"
                try:
                    if not scram.verify_server_final(body[4:]):
                        return False, "SCRAM server signature mismatch"
                except RuntimeError as exc:
                    return False, f"SCRAM failed: {exc}"
                continue  # AuthenticationOk then ReadyForQuery follow
            return False, (f"unsupported auth method {code} "
                           "(only trust/cleartext/md5/scram-sha-256)")
        if tag == b"E":
            return False, f"auth failed: {_error_detail(body)}"
        if tag == b"Z":  # ReadyForQuery
            return True, "authenticated"
        # S (ParameterStatus), K (BackendKeyData), N ... just keep reading
    return False, "auth exchange did not complete"


def _simple_query(sock: socket.socket, timeout: float,
                  sql: str) -> Tuple[List[str], List[List[str]]]:
    """Run one simple query; returns (column_names, rows_as_text)."""
    payload = sql.encode() + b"\x00"
    sock.sendall(b"Q" + (len(payload) + 4).to_bytes(4, "big") + payload)
    columns: List[str] = []
    rows: List[List[str]] = []
    for _ in range(10000):  # bounded: a database list is small
        tag, body = _read_message(sock, timeout)
        if tag == b"T":  # RowDescription
            n = int.from_bytes(body[:2], "big")
            off = 2
            for _ in range(n):
                end = body.index(b"\x00", off)
                columns.append(body[off:end].decode(errors="replace"))
                off = end + 1 + 18  # name + table/col/type/typmod/format fields
        elif tag == b"D":  # DataRow
            n = int.from_bytes(body[:2], "big")
            off = 2
            row: List[str] = []
            for _ in range(n):
                clen = int.from_bytes(body[off:off + 4], "big", signed=True)
                off += 4
                if clen < 0:
                    row.append("")
                else:
                    row.append(body[off:off + clen].decode(errors="replace"))
                    off += clen
            rows.append(row)
        elif tag == b"E":
            raise RuntimeError(_error_detail(body))
        elif tag == b"Z":  # ReadyForQuery - done
            return columns, rows
        # C (CommandComplete), I (EmptyQueryResponse), S, N ... ignore
    raise RuntimeError("query did not complete")


def fetch_databases(host: str, port: int, timeout: float, user: str,
                    password: Optional[str], db: str) -> Dict:
    """Connect with the v3 protocol and list databases. Stdlib only."""
    result: Dict = {"attempted": True, "ok": False, "databases": [], "error": None}
    infos = socket.getaddrinfo(host, port, proto=socket.IPPROTO_TCP)
    sock = None
    last_err: Optional[Exception] = None
    for family, socktype, proto, _canon, sockaddr in infos:
        if socktype != socket.SOCK_STREAM:
            continue
        candidate = None
        try:
            candidate = socket.socket(family, socktype, proto)
            candidate.settimeout(timeout)
            candidate.connect(sockaddr)
            sock = candidate
            break
        except OSError as exc:
            last_err = exc
            if candidate is not None:
                candidate.close()
    if sock is None:
        result["error"] = f"connect failed: {last_err}"
        return result

    try:
        # SSL negotiation, same as the plain probe.
        sock.sendall((8).to_bytes(4, "big") + SSL_REQUEST_CODE.to_bytes(4, "big"))
        reply = _recv_exact(sock, 1, timeout)
        if reply == b"S":
            ctx = ssl.create_default_context()
            ctx.check_hostname = False
            ctx.verify_mode = ssl.CERT_NONE
            sock = ctx.wrap_socket(sock, server_hostname=host)
        elif reply != b"N":
            result["error"] = "port did not answer the PostgreSQL SSLRequest"
            return result

        sock.sendall(_startup_message(user, db))
        ok, detail = _authenticate(sock, timeout, user, password)
        if not ok:
            result["error"] = detail
            return result

        _cols, rows = _simple_query(
            sock, timeout,
            "select datname from pg_database where not datistemplate "
            "order by 1")
        result["ok"] = True
        result["databases"] = [r[0] for r in rows if r]
        return result
    except (OSError, ConnectionError, RuntimeError) as exc:
        result["error"] = str(exc)
        return result
    finally:
        try:
            sock.close()
        except OSError:
            pass


def fetch_databases_psql(host: str, port: int, user: Optional[str],
                         db: Optional[str], timeout: float) -> Optional[List[str]]:
    """Fallback: list databases via a local psql client, if one exists."""
    if not shutil.which("psql"):
        return None
    env = dict(os.environ, PGCONNECT_TIMEOUT=str(int(max(1, timeout))))
    if user:
        env["PGUSER"] = user
    cmd = ["psql", "-h", host, "-p", str(port), "-d", db or "postgres",
           "-X", "-A", "-t", "-c",
           "select datname from pg_database where not datistemplate order by 1"]
    try:
        proc = subprocess.run(cmd, capture_output=True, text=True,
                              env=env, timeout=timeout + 15)
    except (subprocess.TimeoutExpired, OSError):
        return None
    if proc.returncode != 0:
        return None
    return [l.strip() for l in proc.stdout.splitlines() if l.strip()]


# ── SSH probe ────────────────────────────────────────────────────────────


class SSHResult:
    def __init__(self):
        self.available: Optional[bool] = None
        self.error: Optional[str] = None
        self.vars: Dict[str, List[str]] = {}

    def get(self, key: str) -> Optional[str]:
        vals = self.vars.get(key)
        return vals[0] if vals else None

    def get_list(self, key: str) -> List[str]:
        return list(self.vars.get(key, []))

    def as_dict(self) -> Dict:
        return {
            "available": self.available,
            "error": self.error,
            **{k: (v[0] if len(v) == 1 else v) for k, v in self.vars.items()},
        }


def probe_ssh(host: str, user: Optional[str], key_file: Optional[str],
              timeout: float, extra_opts: List[str]) -> SSHResult:
    res = SSHResult()
    script = SSH_PROBE_SCRIPT % {
        "cmds": " ".join(PG_COMMANDS),
        "dirs": " ".join(BIN_SEARCH_DIRS),
    }
    cmd = ["ssh", "-o", "BatchMode=yes", "-o", "ConnectTimeout=%d" % max(1, int(timeout)),
           "-o", "StrictHostKeyChecking=accept-new", "-n"]
    if key_file:
        cmd += ["-i", os.path.expanduser(key_file)]
    cmd += extra_opts
    cmd += [f"{user}@{host}" if user else host, script]

    if not shutil.which("ssh"):
        res.error = "no ssh client on this machine"
        return res

    try:
        proc = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout + 25)
    except subprocess.TimeoutExpired:
        res.error = f"ssh to {host} timed out after {timeout + 25}s"
        return res
    except OSError as exc:
        res.error = f"could not run ssh: {exc}"
        return res

    if proc.returncode != 0:
        err = (proc.stderr or "").strip().splitlines()
        res.error = err[-1] if err else f"ssh exited with status {proc.returncode}"
        # ssh can fail outright, or reach the host and have the script fail.
        res.available = False
        return res

    res.available = True
    for line in (proc.stdout or "").splitlines():
        line = line.strip()
        if not line or "=" not in line:
            continue
        k, v = line.split("=", 1)
        if v == "":
            continue
        res.vars.setdefault(k, []).append(v)
    return res


# ── local probe (host == localhost) ──────────────────────────────────────


def probe_local(timeout: float) -> SSHResult:
    """Run the same discovery locally when the target is the local machine."""
    res = SSHResult()
    script = SSH_PROBE_SCRIPT % {
        "cmds": " ".join(PG_COMMANDS),
        "dirs": " ".join(BIN_SEARCH_DIRS),
    }
    try:
        proc = subprocess.run(["sh", "-c", script], capture_output=True, text=True,
                              timeout=timeout + 25)
        res.available = True
        for line in (proc.stdout or "").splitlines():
            line = line.strip()
            if not line or "=" not in line:
                continue
            k, v = line.split("=", 1)
            if v:
                res.vars.setdefault(k, []).append(v)
    except Exception as exc:  # pragma: no cover - local probe is best effort
        res.available = False
        res.error = str(exc)
    return res


# ── sudo-to-postgres probe ───────────────────────────────────────────────


def _parse_kv_lines(text: str, res: SSHResult) -> None:
    for line in text.splitlines():
        line = line.strip()
        if not line or "=" not in line:
            continue
        k, v = line.split("=", 1)
        if v:
            res.vars.setdefault(k, []).append(v)


def probe_sudo(host: str, user: Optional[str], key_file: Optional[str],
               timeout: float, extra_opts: List[str], pg_os_user: str,
               db: str, sudo_password: Optional[str]) -> SSHResult:
    """Inspect the DB as the postgres OS user via `sudo -u postgres`.

    Works over SSH for remote hosts, or via a local shell for localhost.
    The sudo password (if needed) is fed to `sudo -S` on stdin.
    """
    res = SSHResult()
    script = SUDO_PROBE_SCRIPT % {"pguser": pg_os_user, "db": db}
    is_local = host in ("localhost", "[IP_ADDRESS]", "::1", socket.gethostname())

    if is_local:
        cmd = ["sh", "-c", script]
    else:
        if not shutil.which("ssh"):
            res.error = "no ssh client on this machine"
            return res
        cmd = ["ssh", "-o", "BatchMode=yes",
               "-o", "ConnectTimeout=%d" % max(1, int(timeout)),
               "-o", "StrictHostKeyChecking=accept-new", "-n"]
        if key_file:
            cmd += ["-i", os.path.expanduser(key_file)]
        cmd += extra_opts
        cmd += [f"{user}@{host}" if user else host, script]

    # Feed the sudo password on stdin; the script reads it and primes sudo.
    stdin_data = (sudo_password + "\n") if sudo_password else None

    try:
        proc = subprocess.run(cmd, capture_output=True, text=True,
                              input=stdin_data, timeout=timeout + 30)
    except subprocess.TimeoutExpired:
        res.error = f"sudo probe on {host} timed out"
        return res
    except OSError as exc:
        res.error = f"could not run sudo probe: {exc}"
        return res

    res.available = proc.returncode == 0
    if not res.available:
        err = (proc.stderr or "").strip().splitlines()
        res.error = err[-1] if err else f"probe exited {proc.returncode}"
    _parse_kv_lines(proc.stdout or "", res)
    return res


# ── optional psql enrichment ─────────────────────────────────────────────


def probe_psql(host: str, port: int, user: Optional[str], db: Optional[str],
               timeout: float) -> Optional[Dict[str, str]]:
    """If a psql client exists locally, ask the server for authoritative info."""
    if not shutil.which("psql"):
        return None
    env = dict(os.environ, PGCONNECT_TIMEOUT=str(int(max(1, timeout))))
    if user:
        env["PGUSER"] = user
    sql = ("select version(), current_setting('server_version'), "
           "current_setting('data_directory'), current_setting('port'), "
           "pg_postmaster_start_time(), current_setting('max_connections')")
    cmd = ["psql", "-h", host, "-p", str(port), "-d", db or "postgres",
           "-X", "-A", "-F", "|", "-t", "-c", sql]
    try:
        proc = subprocess.run(cmd, capture_output=True, text=True,
                              env=env, timeout=timeout + 15)
    except (subprocess.TimeoutExpired, OSError):
        return None
    if proc.returncode != 0:
        return None
    line = next((l for l in proc.stdout.splitlines() if "|" in l), "")
    if not line:
        return None
    keys = ["version", "server_version", "data_directory", "port",
            "start_time", "max_connections"]
    return dict(zip(keys, [p.strip() for p in line.split("|")]))


# ── verdict / reporting ──────────────────────────────────────────────────


def build_report(args) -> Dict:
    report: Dict = {"host": args.host, "port": args.port}

    if args.no_ssh:
        ssh = SSHResult()
        ssh.available = None
        ssh.error = "skipped (--no-ssh)"
    elif args.host in ("localhost", "127.0.0.1", "::1", socket.gethostname()):
        ssh = probe_local(args.timeout)
    else:
        ssh = probe_ssh(args.host, args.ssh_user, args.ssh_key, args.timeout,
                        args.ssh_opt)
    net = probe_network(args.host, args.port, args.timeout,
                        args.pg_user, args.pg_database)
    report["network"] = net.as_dict()
    report["ssh"] = ssh.as_dict()

    # ── optional sudo-to-postgres inspection ─────────────
    if args.sudo:
        sudo_pw = args.sudo_password
        if sudo_pw is None and sys.stdin.isatty():
            try:
                import getpass
                sudo_pw = getpass.getpass(
                    f"sudo password for {args.ssh_user or 'user'}@{args.host} "
                    "(Enter to skip): ") or None
            except (EOFError, KeyboardInterrupt):
                sudo_pw = None
        sudo = probe_sudo(args.host, args.ssh_user, args.ssh_key, args.timeout,
                          args.ssh_opt, args.pg_os_user, args.pg_database, sudo_pw)
        report["sudo"] = sudo.as_dict()

    if args.psql:
        sql = probe_psql(args.host, args.port, args.pg_user, args.pg_database,
                         args.timeout)
        if sql:
            report["psql"] = sql

    # ── database list (only worth trying when something answered) ──
    if net.responded or net.postgres_protocol:
        dbs = fetch_databases(args.host, args.port, args.timeout,
                              args.pg_user, args.pg_password, args.pg_database)
        if not dbs["ok"] and dbs["error"] and "password" in (dbs["error"] or ""):
            # No credentials for the wire probe - try a local psql instead.
            via_psql = fetch_databases_psql(args.host, args.port, args.pg_user,
                                            args.pg_database, args.timeout)
            if via_psql is not None:
                dbs = {"attempted": True, "ok": True, "databases": via_psql,
                       "error": None, "via": "local psql"}
        report["databases"] = dbs

    # ── installed? ───────────────────────────────────────
    found_bins = {}
    for cmd_name in PG_COMMANDS:
        val = ssh.get(f"bin.{cmd_name}")
        if val:
            found_bins[cmd_name] = val
    other_paths = sorted(set(ssh.get_list("found")))
    installed_ssh = bool(found_bins or other_paths or ssh.get_list("pkgs")
                         or ssh.get_list("datadir_found")
                         or ssh.get_list("service"))
    net_suggests_installed = bool(net.postgres_protocol or net.responded)

    if ssh.available is True:
        installed: Optional[bool] = installed_ssh or net_suggests_installed
    elif net_suggests_installed:
        installed = True
    else:
        installed = None  # no evidence either way

    # ── running? ─────────────────────────────────────────
    procs = int(ssh.get("pgprocs") or 0)
    services_active = [s for s in ssh.get_list("service") if ":active:" in s]
    listening = [p for p in (ssh.get("listen") or "").split(",") if p]
    running_ssh = bool(procs or services_active or str(args.port) in listening)
    running_net = bool(net.responded or net.postgres_protocol)

    if ssh.available is True and net.tcp_open is not None:
        running: Optional[bool] = running_ssh or running_net
    elif net.tcp_open is not None:
        running = running_net if net.postgres_protocol is not False else False
    elif ssh.available is True:
        running = running_ssh
    else:
        running = None

    report["installed"] = installed
    report["running"] = running
    report["found_binaries"] = found_bins
    report["other_binary_paths"] = other_paths
    report["postmaster_processes"] = procs or None
    report["services_active"] = services_active or None
    report["listening_ports"] = listening or None
    report["psql_available_locally"] = bool(shutil.which("psql"))

    # ── verdict + exit code ─────────────────────────────────
    if installed and running:
        verdict, code = "INSTALLED AND RUNNING", 0
    elif running and not installed:
        # Answering on the port but SSH saw nothing - very likely remote/managed.
        verdict, code = "RUNNING (installation not visible from SSH)", 0
    elif installed and not running:
        verdict, code = "INSTALLED BUT NOT RUNNING", 1
    elif installed is None and running is None:
        verdict, code = "UNKNOWN - host unreachable", 2
    else:
        verdict, code = "NOT INSTALLED", 2
    report["verdict"] = verdict
    report["exit_code"] = code
    return report


def print_report(rep: Dict, args) -> None:
    net = rep.get("network", {})
    ssh = rep.get("ssh", {})

    print()
    print(c(Colors.BOLD, "PostgreSQL host check"))
    print(hr())
    print_kv_rows([
        ("Target host", f"{rep['host']}  ({net.get('resolved_ip') or 'unresolved'})"),
        ("Port", str(rep["port"])),
    ])

    print()
    print(c(Colors.BOLD, "  Verdict"))
    print(f"    {rep['verdict']}")

    # ── installed ────────────────────────────────────────
    print()
    print(c(Colors.BOLD, "  Installed"))
    rows = [("PostgreSQL installed", tick(rep["installed"]))]
    if ssh.get("version"):
        rows.append(("Version (remote)", ssh["version"]))
    if rep.get("found_binaries"):
        rows.append(("On PATH", ", ".join(sorted(rep["found_binaries"]))))
    if rep.get("other_binary_paths"):
        rows.append(("Other binaries", f"{len(rep['other_binary_paths'])} path(s)"))
    if ssh.get("pkgs"):
        pkgs = ssh["pkgs"] if isinstance(ssh["pkgs"], str) else ", ".join(ssh["pkgs"])
        rows.append((f"Packages ({ssh.get('pkg_mgr', '?')})",
                     pkgs if len(pkgs) < 100 else pkgs[:97] + "..."))
    print_kv_rows(rows)

    # ── running ──────────────────────────────────────────
    print()
    print(c(Colors.BOLD, "  Running"))
    rows = [("PostgreSQL running", tick(rep["running"]))]
    if ssh.get("os"):
        rows.append(("Host OS", ssh["os"]))
    rows.append(("Postmaster processes", str(rep.get("postmaster_processes") or 0)
                 if ssh.get("available") else tick(None)))
    if rep.get("services_active"):
        rows.append(("Active services", ", ".join(rep["services_active"])))
    elif ssh.get("service"):
        svcs = ssh["service"]
        svcs = svcs if isinstance(svcs, list) else [svcs]
        rows.append(("Services seen", ", ".join(svcs)))
    if rep.get("listening_ports"):
        rows.append(("Listening ports", ", ".join(rep["listening_ports"])))
    if ssh.get("pgpid"):
        rows.append(("Postmaster PID", f"{ssh['pgpid']} (user {ssh.get('pguser', '?')})"))
    if ssh.get("pgstart"):
        rows.append(("Process started", ssh["pgstart"]))
    if ssh.get("datadir"):
        rows.append(("Data directory", ssh["datadir"]))
    if ssh.get("unixsock"):
        us = ssh["unixsock"]
        rows.append(("Unix socket dir", us if isinstance(us, str) else ", ".join(us)))
    print_kv_rows(rows)

    # ── network probe ────────────────────────────────────
    print()
    print(c(Colors.BOLD, "  Network probe"))
    print_kv_rows([
        ("TCP port open", tick(net.get("tcp_open"))),
        ("PostgreSQL protocol", tick(net.get("postgres_protocol"))),
        ("TLS offered", tick(net.get("ssl_supported"))),
        ("Answers startup", tick(net.get("responding"))),
        ("Probe detail", net.get("detail") or "-"),
    ])

    if rep.get("psql"):
        print()
        print(c(Colors.BOLD, "  Live query (local psql)"))
        print_kv_rows([(k.replace("_", " ").capitalize(), v)
                       for k, v in rep["psql"].items()])

    # ── databases ────────────────────────────────────────
    dbs = rep.get("databases")
    if dbs:
        print()
        print(c(Colors.BOLD, "  Databases"))
        if dbs.get("ok"):
            names = dbs.get("databases") or []
            via = f"  (via {dbs['via']})" if dbs.get("via") else ""
            print_kv_rows([("Count", f"{len(names)}{via}")])
            if names:
                for name in names:
                    print(f"      - {name}")
            else:
                print("      (no user databases)")
        else:
            print_kv_rows([("List available", "no"),
                           ("Reason", dbs.get("error") or "unknown")])

    # ── sudo-to-postgres inspection ──────────────────────
    sudo = rep.get("sudo")
    if sudo:
        print()
        print(c(Colors.BOLD, "  Instance detail (via sudo -u postgres)"))
        if sudo.get("available") is False:
            print_kv_rows([("Probe", "failed"),
                           ("Reason", sudo.get("error") or "unknown")])
        else:
            rows = []
            if sudo.get("sudo"):
                rows.append(("sudo", sudo["sudo"]))
            if sudo.get("pg_version"):
                rows.append(("Version", sudo["pg_version"]))
            if sudo.get("pg_datadir"):
                rows.append(("Data directory", sudo["pg_datadir"]))
            if sudo.get("pg_port"):
                rows.append(("Port", sudo["pg_port"]))
            if sudo.get("pg_start"):
                rows.append(("Up since", sudo["pg_start"]))
            if sudo.get("pg_maxconn"):
                rows.append(("Max connections", sudo["pg_maxconn"]))
            if sudo.get("pg_active"):
                rows.append(("Active queries", sudo["pg_active"]))
            if sudo.get("pg_dbsize"):
                rows.append(("Total size", sudo["pg_dbsize"]))
            if sudo.get("pg_roles"):
                rows.append(("Roles", sudo["pg_roles"]))
            print_kv_rows(rows)
            if sudo.get("pg_dbs"):
                names = [d for d in sudo["pg_dbs"].split(",") if d]
                print_kv_rows([("Databases", str(len(names)))])
                for name in names:
                    print(f"      - {name}")
            if not any(sudo.get(k) for k in ("pg_version", "pg_dbs", "pg_datadir")):
                print_kv_rows([("Result", "sudo worked but psql returned nothing "
                                          "(is the DB up / unix socket reachable?)")])

    if ssh.get("available") is False and ssh.get("error"):
        print()
        print(c(Colors.YELLOW, f"  ! SSH probe unavailable: {ssh['error']}"))
        print(c(Colors.CYAN, "    Network findings only. Pass -U/--ssh-user and "
                             "-i/--ssh-key, or use --no-ssh to silence this."))

    # ── suggestions ──────────────────────────────────────
    hints = build_hints(rep)
    if hints:
        print()
        print(c(Colors.BOLD, "  Next steps"))
        for h in hints:
            print(c(Colors.CYAN, f"    - {h}"))
    print(hr())


def build_hints(rep: Dict) -> List[str]:
    net = rep.get("network", {})
    ssh = rep.get("ssh", {})
    hints: List[str] = []
    if ssh.get("available") is False:
        hints.append(f"Enable key-based SSH: ssh -o StrictHostKeyChecking=accept-new "
                     f"{rep['host']}  (then re-run with -U <user>)")
    if rep["installed"] and not rep["running"]:
        svc = (ssh.get("service") or ["postgresql"])[0]
        svc = svc.split(":")[0] if isinstance(svc, str) else "postgresql"
        hints.append(f"Start it:  ssh {rep['host']} 'sudo systemctl start {svc}'")
        hints.append(f"Or:        ssh {rep['host']} 'pg_ctl -D "
                     f"{ssh.get('datadir', '/var/lib/pgsql/data')} start'")
    if not rep["installed"] and rep["running"] is None:
        hints.append("Host unreachable - check the name, firewall, and that port "
                     f"{rep['port']} is open")
    if net.get("tcp_open") and net.get("postgres_protocol") is False:
        hints.append(f"Port {rep['port']} is open but is not PostgreSQL - confirm the "
                     "port with --port")
    if rep["running"] and not rep.get("psql") and not rep.get("psql_available_locally"):
        hints.append("Install psql locally (brew install libpq / apt install "
                     "postgresql-client) and pass --psql for live server details")
    dbs = rep.get("databases")
    if rep["running"] and dbs and not dbs.get("ok") and "password" in (dbs.get("error") or ""):
        hints.append("List databases: re-run with --pg-password <pw> "
                     "(or --pg-user/-U as needed)")
    return hints


def print_kv_rows(rows: List[Tuple[str, str]]) -> None:
    rows = [(k, v) for k, v in rows if v is not None]
    if not rows:
        return
    width = max(len(k) for k, _ in rows)
    for k, v in rows:
        # pad using the plain key length, values may contain ANSI codes
        print(f"    {k:<{width}}  {v}")


# ── CLI ──────────────────────────────────────────────────────────────────


def parse_args(argv: Optional[List[str]] = None):
    parser = argparse.ArgumentParser(
        prog="check_postgres.py",
        description="Show whether PostgreSQL is installed and/or running on a host.",
        epilog="The hostname is the final parameter, per this repo's convention.",
    )
    parser.add_argument("host", help="Target hostname or IP (final parameter)")
    parser.add_argument("-p", "--port", type=int, default=DEFAULT_PORT,
                        help=f"PostgreSQL port to probe (default: {DEFAULT_PORT})")
    parser.add_argument("-U", "--ssh-user", default=pc.env(pc.ENV_SSH_USER),
                        help=f"SSH user for the host probe (default: ${pc.ENV_SSH_USER})")
    parser.add_argument("-i", "--ssh-key", default=pc.env(pc.ENV_SSH_KEY),
                        help=f"SSH private key path (default: ${pc.ENV_SSH_KEY})")
    parser.add_argument("-o", "--ssh-opt", action="append", default=[], metavar="OPT",
                        help="Extra raw ssh option, repeatable (e.g. -o ProxyJump=bastion)")
    parser.add_argument("--no-ssh", action="store_true",
                        help="Skip the SSH host probe; network probe only")
    parser.add_argument("--psql", action="store_true",
                        help="Also query the server with a local psql if one exists")
    parser.add_argument("--pg-user", default=pc.env(pc.ENV_PG_USER, pc.DEFAULT_PG_USER),
                        help=f"User name for the protocol/psql probe (default: ${pc.ENV_PG_USER} or postgres)")
    parser.add_argument("--pg-database", default=pc.env(pc.ENV_PG_DATABASE, pc.DEFAULT_PG_DATABASE),
                        help=f"Database for the protocol/psql probe (default: ${pc.ENV_PG_DATABASE} or postgres)")
    parser.add_argument("--pg-password", default=pc.env(pc.ENV_PG_PASSWORD),
                        help=f"Password for the database-listing probe "
                             f"(default: ${pc.ENV_PG_PASSWORD}; omit for trust auth)")
    parser.add_argument("--sudo", action="store_true",
                        help="Inspect the instance as the postgres OS user via "
                             "sudo over SSH (unix-socket psql; works even if the "
                             "TCP port is closed)")
    parser.add_argument("--sudo-password", default=pc.env(pc.ENV_SUDO_PASSWORD),
                        help=f"sudo password (default: ${pc.ENV_SUDO_PASSWORD}; prompted "
                             "interactively if --sudo is given and none is set)")
    parser.add_argument("--pg-os-user", default=pc.env(pc.ENV_PG_OS_USER, pc.DEFAULT_PG_OS_USER),
                        help=f"postgres OS account to sudo to (default: ${pc.ENV_PG_OS_USER} or postgres)")
    parser.add_argument("-t", "--timeout", type=float, default=CONNECT_TIMEOUT,
                        help=f"Per-probe timeout in seconds (default: {CONNECT_TIMEOUT})")
    parser.add_argument("--json", action="store_true", help="Emit machine-readable JSON")
    parser.add_argument("--no-color", action="store_true", help="Disable ANSI colour")
    parser.add_argument("-q", "--quiet", action="store_true",
                        help="Print only the verdict line")
    parser.add_argument("--version", action="version",
                        version="check_postgres.py v1.0")
    return parser.parse_args(argv)


def main(argv: Optional[List[str]] = None) -> int:
    args = parse_args(argv)
    if args.no_color or not sys.stdout.isatty() or args.json:
        Colors.disable()
    try:
        rep = build_report(args)
    except KeyboardInterrupt:
        print("\nCancelled.", file=sys.stderr)
        return 3
    except Exception as exc:  # pragma: no cover
        print(f"Error: {exc}", file=sys.stderr)
        return 3

    if args.json:
        print(json.dumps(rep, indent=2, default=str))
    elif args.quiet:
        print(f"{rep['host']}:{rep['port']}  {rep['verdict']}")
    else:
        print_report(rep, args)
    return rep["exit_code"]


if __name__ == "__main__":
    sys.exit(main())
