#!/usr/bin/env python3
"""
Script to show current configuration of XRAY PostgreSQL database
Usage: python3 show_xray_db_config.py [--env-file <path>] [--exact-counts]

Connects remotely via psycopg2 (database) and paramiko (SSH for OS commands).
"""

import os
import sys
import psycopg2
import psycopg2.extras
import paramiko
import argparse
import json
from typing import Optional, List, Tuple


class Colors:
    RED = "\033[0;31m"
    GREEN = "\033[0;32m"
    MAGENTA = "\033[0;35m"
    BLUE = "\033[0;34m"
    BOLD_BLUE = "\033[1;34m"
    BOLD = "\033[1m"
    NC = "\033[0m"


def print_table(headers: List[str], rows: List[List[str]]):
    """Print an aligned table with column headers."""
    if not rows:
        return
    widths = [len(h) for h in headers]
    for row in rows:
        for i, val in enumerate(row):
            if i < len(widths):
                widths[i] = max(widths[i], len(str(val)))
    fmt = "  ".join(f"{{:<{w}}}" for w in widths)
    print(f"  {fmt.format(*headers)}")
    print(f"  {'  '.join('─' * w for w in widths)}")
    for row in rows:
        padded = [str(v) for v in row] + [""] * (len(headers) - len(row))
        print(f"  {fmt.format(*padded[:len(headers)])}")


def print_kv(pairs: List[Tuple[str, str]], indent: int = 2):
    """Print key-value pairs aligned on the colon."""
    if not pairs:
        return
    max_key = max(len(k) for k, _ in pairs)
    for key, val in pairs:
        print(f"{' ' * indent}{key:<{max_key}}  {val}")


def print_side_by_side(
    left_headers: List[str],
    left_rows: List[List[str]],
    right_headers: List[str],
    right_rows: List[List[str]],
    gutter: str = " │ ",
):
    """Print two tables side by side separated by a gutter."""

    def col_widths(headers, rows):
        widths = [len(h) for h in headers]
        for row in rows:
            for i, val in enumerate(row):
                if i < len(widths):
                    widths[i] = max(widths[i], len(str(val)))
        return widths

    lw = col_widths(left_headers, left_rows)
    rw = col_widths(right_headers, right_rows)
    lfmt = "  ".join(f"{{:<{w}}}" for w in lw)
    rfmt = "  ".join(f"{{:<{w}}}" for w in rw)
    left_total_w = sum(lw) + 2 * (len(lw) - 1)

    def left_line(cells):
        padded = [str(v) for v in cells] + [""] * (len(left_headers) - len(cells))
        return lfmt.format(*padded[: len(left_headers)])

    def right_line(cells):
        padded = [str(v) for v in cells] + [""] * (len(right_headers) - len(cells))
        return rfmt.format(*padded[: len(right_headers)])

    blank_left = " " * left_total_w

    print(f"  {left_line(left_headers)}{gutter}{right_line(right_headers)}")
    print(f"  {'  '.join('─' * w for w in lw)}{gutter}{'  '.join('─' * w for w in rw)}")

    max_rows = max(len(left_rows), len(right_rows))
    for i in range(max_rows):
        l = left_line(left_rows[i]) if i < len(left_rows) else blank_left
        r = right_line(right_rows[i]) if i < len(right_rows) else ""
        print(f"  {l}{gutter}{r}")


class XrayDBConfig:
    def __init__(
        self, use_exact_counts: bool = False, xray_env_file: str = "./xray_env"
    ):
        self.xray_env_file = xray_env_file
        self.env_vars = {}
        self.db_name = None
        self.db_tablespace = None
        self.pg_connection = None
        self.db_connection = None
        self.use_exact_counts = use_exact_counts

        self.pg_host = "localhost"
        self.pg_port = 5432
        self.pg_user = "postgres"
        self.pg_password = None
        self.sizes_only = False
        self.verbose = False
        self.output_file = None
        self.json_data = {}

        self.remote_user = None
        self.remote_ssh_key = None
        self._ssh_client = None

    # ── helpers ──────────────────────────────────────────

    def _get_ssh_client(self) -> paramiko.SSHClient:
        if self._ssh_client is not None:
            transport = self._ssh_client.get_transport()
            if transport and transport.is_active():
                return self._ssh_client

        if not self.remote_user or not self.remote_ssh_key:
            raise RuntimeError(
                "REMOTE_USER and REMOTE_SSH_KEY must be set in the env file"
            )

        client = paramiko.SSHClient()
        client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        client.connect(
            hostname=self.pg_host,
            username=self.remote_user,
            key_filename=self.remote_ssh_key,
        )
        self._ssh_client = client
        return client

    def run_remote_cmd(self, cmd: str) -> Tuple[str, str, int]:
        client = self._get_ssh_client()
        stdin, stdout, stderr = client.exec_command(cmd)
        exit_status = stdout.channel.recv_exit_status()
        return stdout.read().decode(), stderr.read().decode(), exit_status

    def _remote_path_exists(self, path: str) -> bool:
        _, _, rc = self.run_remote_cmd(f"test -e {path}")
        return rc == 0

    def _remote_is_symlink(self, path: str) -> bool:
        _, _, rc = self.run_remote_cmd(f"test -L {path}")
        return rc == 0

    def _remote_is_dir(self, path: str) -> bool:
        _, _, rc = self.run_remote_cmd(f"test -d {path}")
        return rc == 0

    def _remote_readlink(self, path: str) -> Optional[str]:
        stdout, _, rc = self.run_remote_cmd(f"readlink -f {path}")
        return stdout.strip() if rc == 0 and stdout.strip() else None

    def _remote_dir_size(self, path: str) -> str:
        try:
            stdout, _, rc = self.run_remote_cmd(f"du -sh {path}")
            if rc == 0 and stdout.strip():
                return stdout.split()[0]
        except Exception:
            pass
        return "?"

    def _c(self, color: str, msg: str) -> str:
        return f"{color}{msg}{Colors.NC}"

    def _ok(self, msg: str):
        print(self._c(Colors.GREEN, f"  ✓ {msg}"))

    def _warn(self, msg: str):
        print(self._c(Colors.MAGENTA, f"  ⚠ {msg}"))

    def _err(self, msg: str):
        print(self._c(Colors.RED, f"  ✗ {msg}"))

    def _section(self, title: str):
        print()
        print(self._c(Colors.BOLD_BLUE, f"── {title} ──"))

    # ── database connectivity ────────────────────────────

    def connect_to_postgres(
        self, database: str = "postgres"
    ) -> Optional[psycopg2.extensions.connection]:
        params = {
            "host": self.pg_host,
            "port": self.pg_port,
            "database": database,
            "user": self.pg_user,
        }
        if self.pg_password:
            params["password"] = self.pg_password
        try:
            return psycopg2.connect(**params)
        except psycopg2.Error as e:
            self._warn(f"Could not connect to '{database}': {e}")
            return None

    def close_connections(self):
        for conn in (self.pg_connection, self.db_connection):
            if conn:
                try:
                    conn.close()
                except Exception:
                    pass
        if self._ssh_client:
            try:
                self._ssh_client.close()
            except Exception:
                pass

    def _query(self, query: str, database: str = "postgres") -> Optional[List[Tuple]]:
        connection = None
        if database == "postgres":
            if not self.pg_connection:
                self.pg_connection = self.connect_to_postgres(database)
            connection = self.pg_connection
        elif database == self.db_name:
            if not self.db_connection:
                self.db_connection = self.connect_to_postgres(database)
            connection = self.db_connection
        else:
            connection = self.connect_to_postgres(database)

        if connection:
            try:
                with connection.cursor() as cur:
                    cur.execute(query)
                    return cur.fetchall()
            except psycopg2.Error as e:
                connection.rollback()
                self._warn(f"Query error: {e}")
                return None

        # Fallback: psql over SSH
        try:
            escaped = query.replace("'", "'\\''")
            stdout, _, rc = self.run_remote_cmd(
                f"sudo -u postgres psql -d {database} -t -c '{escaped}'"
            )
            if rc == 0 and stdout.strip():
                return [
                    tuple(col.strip() for col in line.split("|"))
                    for line in stdout.strip().split("\n")
                    if line.strip()
                ]
            return []
        except Exception as e:
            self._warn(f"Query error: {e}")
            return None

    def _query_single(self, query: str, database: str = "postgres") -> Optional[Tuple]:
        result = self._query(query, database)
        return result[0] if result else None

    # ── environment loading ──────────────────────────────

    def load_xray_environment(self) -> bool:
        if not os.path.exists(self.xray_env_file):
            self._err(f"Environment file not found: {self.xray_env_file}")
            self._err(
                "An environment file is required. Use --env-file <path> to specify one."
            )
            print()
            print("  The env file must define at least these variables:")
            print("    export XRAY_PG_DB_NAME=<database_name>")
            print("    export XRAY_PG_DB_TS=<tablespace_name>")
            print("    export XRAY_DB_SERVER=<remote_db_hostname>")
            print("    export PGHOST=$XRAY_DB_SERVER")
            print("    export PGUSER=postgres")
            print("    export REMOTE_USER=<ssh_username>")
            print("    export REMOTE_SSH_KEY=<path_to_ssh_private_key>")
            print()
            print(
                "  Example: python3 show_xray_db_config.py --env-file ./xray_set_env_8disklocal"
            )
            return False

        try:
            import re

            with open(self.xray_env_file, "r") as f:
                for line in f:
                    line = line.strip()
                    if line and not line.startswith("#") and "=" in line:
                        if line.startswith("export "):
                            line = line[7:]
                        if "=" in line:
                            key, value = line.split("=", 1)
                            key = key.strip()
                            value = value.strip()
                            if (value.startswith('"') and value.endswith('"')) or (
                                value.startswith("'") and value.endswith("'")
                            ):
                                value = value[1:-1]
                            if "$" in value:

                                def replace_var(match):
                                    vn = match.group(1)
                                    return self.env_vars.get(
                                        vn, os.environ.get(vn, match.group(0))
                                    )

                                value = re.sub(
                                    r"\$([A-Za-z_][A-Za-z0-9_]*)", replace_var, value
                                )
                            self.env_vars[key] = value
                            os.environ[key] = value

            self.db_name = self.env_vars.get("XRAY_PG_DB_NAME")
            self.db_tablespace = self.env_vars.get("XRAY_PG_DB_TS")
            self.pg_host = self.env_vars.get("PGHOST", self.pg_host)
            self.pg_port = int(self.env_vars.get("PGPORT", self.pg_port))
            self.pg_user = self.env_vars.get("PGUSER", self.pg_user)
            self.pg_password = self.env_vars.get("PGPASSWORD", self.pg_password)
            self.remote_user = self.env_vars.get("REMOTE_USER")
            self.remote_ssh_key = self.env_vars.get("REMOTE_SSH_KEY")

            if not self.db_name:
                self._err("XRAY_PG_DB_NAME not set in env file")
                return False

            return True
        except Exception as e:
            self._err(f"Error reading env file: {e}")
            return False

    # ── data collection ──────────────────────────────────

    def _get_data_directory(self) -> Optional[str]:
        if not self.pg_connection:
            self.pg_connection = self.connect_to_postgres()
        if self.pg_connection:
            try:
                with self.pg_connection.cursor() as cur:
                    cur.execute("SHOW data_directory;")
                    r = cur.fetchone()
                    return r[0] if r else None
            except psycopg2.Error:
                self.pg_connection.rollback()
        row = self._query_single("SHOW data_directory;")
        return row[0] if row else None

    def _get_fs_info(self, path: str) -> dict:
        """Return filesystem info dict for a remote path."""
        info = {}
        try:
            stdout, _, rc = self.run_remote_cmd(f"df -h {path}")
            if rc == 0:
                lines = stdout.strip().split("\n")
                if len(lines) >= 2:
                    d = lines[1].split()
                    if len(d) >= 6:
                        info["device"] = d[0]
                        info["size"] = d[1]
                        info["used"] = d[2]
                        info["avail"] = d[3]
                        info["use%"] = d[4]
                        info["mount"] = " ".join(d[5:])
        except Exception:
            pass
        return info

    def _get_dir_stats(self, path: str) -> dict:
        """Return owner/group/perms/size for a remote directory."""
        info = {}
        try:
            stdout, _, rc = self.run_remote_cmd(f"stat -c '%U %G %a' {path}")
            if rc == 0 and stdout.strip():
                parts = stdout.strip().split()
                if len(parts) >= 3:
                    info["owner"] = parts[0]
                    info["group"] = parts[1]
                    info["perms"] = parts[2]
            info["size"] = self._remote_dir_size(path)
        except Exception:
            pass
        return info

    # ── output methods ───────────────────────────────────

    def check_service(self):
        """Verify PostgreSQL is running; show connection details if verbose."""
        # Service check (always runs)
        try:
            stdout, _, rc = self.run_remote_cmd("pgrep -c postgres")
            count = int(stdout.strip()) if rc == 0 else 0
        except Exception:
            count = 0
        if count == 0:
            self._err("PostgreSQL is NOT running on remote host")
            sys.exit(1)

        svc = {
            "host": self.pg_host,
            "ssh_user": self.remote_user or "?",
            "pg_user": self.pg_user,
            "database": self.db_name,
            "tablespace": self.db_tablespace or "(default)",
            "pg_processes": count,
        }

        if not self.pg_connection:
            self.pg_connection = self.connect_to_postgres()
        if self.pg_connection:
            try:
                with self.pg_connection.cursor() as cur:
                    cur.execute("SELECT version();")
                    ver = cur.fetchone()[0].split(",")[0]
                    svc["version"] = ver
            except Exception:
                pass

        self.json_data["service"] = svc

        if not self.verbose:
            return

        self._section("Connection & Service")
        print_kv(
            [
                ("Host:", svc["host"]),
                ("SSH User:", svc["ssh_user"]),
                ("PG User:", svc["pg_user"]),
                ("Database:", svc["database"]),
                ("Tablespace:", svc["tablespace"]),
            ]
        )
        self._ok(f"PostgreSQL running ({count} processes)")
        if "version" in svc:
            print(f"  Version:  {svc['version']}")

    def check_database_exists(self):
        """Verify the target database exists; exits if not."""
        row = self._query_single(
            "SELECT datname FROM pg_database WHERE datname = '%s';" % self.db_name
        )
        if not row:
            self._err(f"Database '{self.db_name}' does not exist")
            sys.exit(1)

    def show_database_info(self):
        """Show database details (verbose only)."""
        self._section("Database")

        size_expr = (
            "pg_size_pretty(pg_database_size(current_database()))"
            if self.use_exact_counts
            else """pg_size_pretty(COALESCE(
                (SELECT SUM(pg_total_relation_size(c.oid))
                 FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
                 WHERE n.nspname NOT IN ('information_schema','pg_catalog','pg_toast')),
                pg_database_size(current_database())))"""
        )

        row = self._query_single(
            f"""
        SELECT
            {size_expr},
            (SELECT count(*) FROM pg_stat_activity WHERE datname = current_database()),
            pg_encoding_to_char(encoding),
            datcollate
        FROM pg_database WHERE datname = current_database();
        """,
            self.db_name,
        )

        if row:
            tag = "exact" if self.use_exact_counts else "est."
            db_info = {
                "size": row[0],
                "size_type": tag,
                "connections": int(row[1]),
                "encoding": row[2],
                "collation": row[3],
            }

            kv = [
                ("Size:", f"{row[0]} ({tag})"),
                ("Connections:", str(row[1])),
                ("Encoding:", row[2]),
                ("Collation:", row[3]),
            ]

            data_dir = self._get_data_directory()
            if data_dir:
                db_info["data_dir"] = data_dir
                wal_path = f"{data_dir}/pg_wal"
                wal_target = None
                wal_type = "directory"
                if self._remote_is_symlink(wal_path):
                    wal_target = self._remote_readlink(wal_path)
                    wal_type = "symlink"
                effective_wal = wal_target or wal_path
                wal_size = (
                    self._remote_dir_size(effective_wal)
                    if self._remote_path_exists(effective_wal)
                    else "?"
                )
                wal_files = "?"
                if self._remote_path_exists(effective_wal):
                    stdout, _, rc = self.run_remote_cmd(
                        f"ls {effective_wal}/0* 2>/dev/null | wc -l"
                    )
                    if rc == 0:
                        wal_files = stdout.strip()

                db_info["wal_dir"] = wal_target or wal_path
                db_info["wal_symlink"] = wal_target is not None
                db_info["wal_size"] = wal_size
                db_info["wal_files"] = wal_files

                kv.append(("Data Dir:", data_dir))
                if wal_target:
                    kv.append(("WAL Dir:", f"{wal_path} -> {wal_target} ({wal_type})"))
                else:
                    kv.append(("WAL Dir:", wal_path))
                kv.append(("WAL:", f"{wal_size}, {wal_files} files"))

            self.json_data["database"] = db_info
            print_kv(kv)

    def show_tablespace(self) -> Optional[str]:
        self._section("Tablespace")

        row = self._query_single(
            f"""
        SELECT
            COALESCE(t.spcname, 'pg_default'),
            COALESCE(pg_tablespace_location(t.oid), ''),
            CASE WHEN t.spcname IS NULL THEN 'default' ELSE 'custom' END
        FROM pg_database d
        LEFT JOIN pg_tablespace t ON d.dattablespace = t.oid
        WHERE d.datname = '{self.db_name}';
        """
        )

        if not row:
            self._warn("Could not retrieve tablespace info")
            return None

        actual_ts = row[0]
        ts_location = row[1] or "(default data dir)"
        ts_type = row[2]

        ts_info = {
            "name": actual_ts,
            "type": ts_type,
            "location": ts_location,
        }

        print_kv(
            [
                ("Name:", actual_ts),
                ("Type:", ts_type),
                ("Location:", ts_location),
            ]
        )

        # Tablespace mismatch check
        if self.db_tablespace:
            if actual_ts != self.db_tablespace:
                ts_info["mismatch"] = f"config='{self.db_tablespace}', actual='{actual_ts}'"
                self._err(
                    f"MISMATCH: config says '{self.db_tablespace}', database uses '{actual_ts}'"
                )

        # Filesystem info for the tablespace path
        location = ts_location
        if location == "(default data dir)":
            data_dir = self._get_data_directory()
            location = f"{data_dir}/base" if data_dir else None

        # Tablespace size from PostgreSQL
        ts_size_row = self._query_single(
            f"SELECT pg_size_pretty(pg_tablespace_size('{actual_ts}'));"
        )
        if ts_size_row:
            ts_info["ts_size"] = ts_size_row[0]
            print_kv([("TS Size:", ts_size_row[0])])

        if location and self._remote_path_exists(location):
            fs = self._get_fs_info(location)
            ds = self._get_dir_stats(location)
            if fs or ds:
                rows = []
                if fs:
                    ts_info["filesystem"] = f"{fs.get('device','')} on {fs.get('mount','')}"
                    ts_info["disk"] = {
                        "used": fs.get("used", "?"),
                        "size": fs.get("size", "?"),
                        "use_pct": fs.get("use%", "?"),
                    }
                    rows.append(
                        ["Filesystem", f"{fs.get('device','')} on {fs.get('mount','')}"]
                    )
                    rows.append(
                        [
                            "Disk (df)",
                            f"{fs.get('used','?')} / {fs.get('size','?')} ({fs.get('use%','?')})",
                        ]
                    )
                if ds:
                    ts_info["owner"] = f"{ds.get('owner','?')}:{ds.get('group','?')} ({ds.get('perms','?')})"
                    rows.append(
                        [
                            "Owner",
                            f"{ds.get('owner','?')}:{ds.get('group','?')} ({ds.get('perms','?')})",
                        ]
                    )
                print_kv([(r[0] + ":", r[1]) for r in rows])

                # Disk usage warning
                try:
                    pct = float(fs.get("use%", "0").rstrip("%"))
                    if pct >= 90:
                        self._err(f"Disk usage {fs['use%']} - critically high!")
                    elif pct >= 80:
                        self._warn(f"Disk usage {fs['use%']} - getting high")
                except (ValueError, KeyError):
                    pass

        self.json_data["tablespace"] = ts_info
        return actual_ts

    def show_tablespace_objects(self):
        """Show top tables and indexes in the XRAY tablespace."""
        ts = self.db_tablespace
        if not ts:
            return

        self._section(f"Objects in Tablespace '{ts}'")

        ts_filter = f"""(
            c.reltablespace = (SELECT oid FROM pg_tablespace WHERE spcname = '{ts}')
            OR (c.reltablespace = 0
                AND (SELECT dattablespace FROM pg_database WHERE datname = current_database())
                    = (SELECT oid FROM pg_tablespace WHERE spcname = '{ts}'))
        )"""

        tables = (
            self._query(
                f"""
        SELECT n.nspname, c.relname,
               pg_size_pretty(pg_total_relation_size(c.oid)),
               LEAST(c.reltuples, 1e15)::bigint
        FROM pg_class c
        JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE c.relkind = 'r'
          AND n.nspname NOT IN ('information_schema','pg_catalog','pg_toast')
          AND {ts_filter}
        ORDER BY pg_total_relation_size(c.oid) DESC LIMIT 10;
        """,
                self.db_name,
            )
            or []
        )

        indexes = (
            self._query(
                f"""
        SELECT c.relname,
               pg_size_pretty(pg_relation_size(c.oid)),
               tc.relname
        FROM pg_class c
        JOIN pg_namespace n ON n.oid = c.relnamespace
        LEFT JOIN pg_index i ON i.indexrelid = c.oid
        LEFT JOIN pg_class tc ON tc.oid = i.indrelid
        WHERE c.relkind = 'i'
          AND n.nspname NOT IN ('information_schema','pg_catalog','pg_toast')
          AND {ts_filter}
        ORDER BY pg_relation_size(c.oid) DESC LIMIT 10;
        """,
                self.db_name,
            )
            or []
        )

        if self.use_exact_counts:
            tables = [
                (r[0], r[1], r[2], self._exact_row_count(r[0], r[1]))
                for r in tables
            ]

        tag = "exact" if self.use_exact_counts else "est."
        row_label = "Rows" if self.use_exact_counts else "Est.Rows"
        left_rows = [[r[0], r[1], r[2], f"{r[3]:,}"] for r in tables]
        right_rows = [[r[0], r[1], r[2] or "?"] for r in indexes]

        self.json_data["tablespace_objects"] = {
            "tables": [
                {"schema": r[0], "table": r[1], "size": r[2], "rows": int(r[3]),
                 "rows_type": tag}
                for r in tables
            ],
            "indexes": [
                {"index": r[0], "size": r[1], "on_table": r[2] or "?"}
                for r in indexes
            ],
        }

        if left_rows or right_rows:
            print_side_by_side(
                ["Schema", "Table", "Size", row_label],
                left_rows,
                ["Index", "Size", "On Table"],
                right_rows,
            )
        else:
            print("  (no tables or indexes)")

    def collect_pg_settings(self):
        """Always collect pg_settings into json_data for output."""
        result = self._query(
            """
        SELECT name,
               setting || COALESCE(' ' || NULLIF(unit,''), '') as value
        FROM pg_settings
        WHERE name IN (
            'shared_buffers','effective_cache_size','work_mem',
            'maintenance_work_mem','max_wal_size','min_wal_size',
            'wal_level','archive_mode','checkpoint_completion_target'
        ) ORDER BY name;
        """
        )
        if result:
            self.json_data["pg_settings"] = {r[0]: r[1] for r in result}
        return result

    def show_pg_settings(self):
        self._section("Key PostgreSQL Settings")
        result = self.collect_pg_settings()
        if result:
            print_table(["Setting", "Value"], [[r[0], r[1]] for r in result])

    def _exact_row_count(self, schema: str, table: str) -> int:
        """Run SELECT count(*) on a single table and return the result."""
        row = self._query_single(
            f'SELECT count(*) FROM "{schema}"."{table}";', self.db_name
        )
        return int(row[0]) if row else 0

    def show_top_tables_and_indexes(self):
        self._section("Top 5 Tables & Indexes")

        tq = """
        SELECT t.schemaname, t.tablename,
               pg_size_pretty(pg_total_relation_size(t.schemaname||'.'||t.tablename)),
               LEAST(c.reltuples, 1e15)::bigint
        FROM pg_tables t
        JOIN pg_class c ON c.relname = t.tablename
        JOIN pg_namespace n ON n.oid = c.relnamespace AND n.nspname = t.schemaname
        WHERE t.schemaname NOT IN ('information_schema','pg_catalog')
        ORDER BY pg_total_relation_size(t.schemaname||'.'||t.tablename) DESC LIMIT 5;
        """

        iq = """
        SELECT c.relname,
               pg_size_pretty(pg_relation_size(c.oid)),
               tc.relname
        FROM pg_class c
        JOIN pg_namespace n ON n.oid = c.relnamespace
        LEFT JOIN pg_index i ON i.indexrelid = c.oid
        LEFT JOIN pg_class tc ON tc.oid = i.indrelid
        WHERE c.relkind = 'i'
          AND n.nspname NOT IN ('information_schema','pg_catalog','pg_toast')
        ORDER BY pg_relation_size(c.oid) DESC LIMIT 5;
        """

        tables = self._query(tq, self.db_name) or []
        indexes = self._query(iq, self.db_name) or []

        if self.use_exact_counts:
            tables = [
                (r[0], r[1], r[2], self._exact_row_count(r[0], r[1]))
                for r in tables
            ]

        tag = "exact" if self.use_exact_counts else "est."

        self.json_data["top_tables"] = [
            {"schema": r[0], "table": r[1], "size": r[2], "rows": int(r[3])}
            for r in tables
        ]
        self.json_data["top_indexes"] = [
            {"index": r[0], "size": r[1], "on_table": r[2] or "?"}
            for r in indexes
        ]

        left_rows = [[r[0], r[1], r[2], f"{r[3]:,}"] for r in tables]
        right_rows = [[r[0], r[1], r[2] or "?"] for r in indexes]

        if left_rows or right_rows:
            print_side_by_side(
                ["Schema", "Table", "Size", f"Rows ({tag})"],
                left_rows,
                ["Index", "Size", "On Table"],
                right_rows,
            )
        else:
            print("  (no tables or indexes)")

    def show_sizes(self):
        """Print only size information: database, tables, and indexes."""
        self._section("Database Size")
        row = self._query_single(
            "SELECT pg_size_pretty(pg_database_size(current_database())), "
            "pg_database_size(current_database());",
            self.db_name,
        )
        if row:
            self.json_data["database_size"] = {
                "pretty": row[0], "bytes": int(row[1]),
            }
            print_kv([("On-disk:", f"{row[0]} ({row[1]} bytes)")])

        self._section("All Tables")
        result = self._query(
            """
        SELECT n.nspname, c.relname,
               pg_size_pretty(pg_total_relation_size(c.oid)),
               pg_total_relation_size(c.oid) as raw_bytes,
               LEAST(c.reltuples, 1e15)::bigint
        FROM pg_class c
        JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE c.relkind = 'r'
          AND n.nspname NOT IN ('information_schema','pg_catalog','pg_toast')
        ORDER BY pg_total_relation_size(c.oid) DESC;
        """,
            self.db_name,
        )

        if result:
            if self.use_exact_counts:
                result = [
                    (r[0], r[1], r[2], r[3], self._exact_row_count(r[0], r[1]))
                    for r in result
                ]

            total_bytes = sum(int(r[3]) for r in result)
            total_rows = sum(int(r[4]) for r in result)
            tag = "exact" if self.use_exact_counts else "est."
            row_label = "Rows" if self.use_exact_counts else "Est.Rows"
            self.json_data["all_tables"] = [
                {"schema": r[0], "table": r[1], "size": r[2],
                 "size_bytes": int(r[3]), "rows": int(r[4]),
                 "rows_type": tag}
                for r in result
            ]
            self.json_data["tables_total"] = {
                "count": len(result), "bytes": total_bytes, "rows": total_rows,
                "rows_type": tag,
            }
            table_rows = [[r[0], r[1], r[2], f"{int(r[4]):,}"] for r in result]
            print_table(["Schema", "Table", "Size", row_label], table_rows)
            print(f"  {'─' * 40}")
            prefix = "" if self.use_exact_counts else "~"
            print(
                f"  Total: {len(result)} tables, "
                f"{self._pretty_bytes(total_bytes)}, "
                f"{prefix}{total_rows:,} rows"
            )

        self._section("All Indexes")
        result = self._query(
            """
        SELECT n.nspname, c.relname,
               pg_size_pretty(pg_relation_size(c.oid)),
               pg_relation_size(c.oid) as raw_bytes,
               tc.relname
        FROM pg_class c
        JOIN pg_namespace n ON n.oid = c.relnamespace
        LEFT JOIN pg_index i ON i.indexrelid = c.oid
        LEFT JOIN pg_class tc ON tc.oid = i.indrelid
        WHERE c.relkind = 'i'
          AND n.nspname NOT IN ('information_schema','pg_catalog','pg_toast')
        ORDER BY pg_relation_size(c.oid) DESC;
        """,
            self.db_name,
        )

        if result:
            total_idx_bytes = sum(int(r[3]) for r in result)
            self.json_data["all_indexes"] = [
                {"schema": r[0], "index": r[1], "size": r[2],
                 "size_bytes": int(r[3]), "on_table": r[4] or "?"}
                for r in result
            ]
            self.json_data["indexes_total"] = {
                "count": len(result), "bytes": total_idx_bytes,
            }
            idx_rows = [[r[0], r[1], r[2], r[4] or "?"] for r in result]
            print_table(["Schema", "Index", "Size", "On Table"], idx_rows)
            print(f"  {'─' * 40}")
            print(
                f"  Total: {len(result)} indexes, "
                f"{self._pretty_bytes(total_idx_bytes)}"
            )

        self._section("Table Bloat")
        bloat = self._query(
            """
        SELECT
            schemaname,
            relname,
            n_dead_tup,
            n_live_tup,
            ROUND(n_dead_tup::numeric / GREATEST(n_live_tup, 1) * 100, 2),
            last_autovacuum,
            last_vacuum
        FROM pg_stat_user_tables
        ORDER BY n_dead_tup DESC;
        """,
            self.db_name,
        )

        if bloat:
            total_dead = sum(int(r[2]) for r in bloat)
            total_live = sum(int(r[3]) for r in bloat)
            self.json_data["table_bloat"] = [
                {
                    "schema": r[0], "table": r[1],
                    "dead_tuples": int(r[2]), "live_tuples": int(r[3]),
                    "bloat_pct": float(r[4]),
                    "last_autovacuum": str(r[5]) if r[5] else None,
                    "last_vacuum": str(r[6]) if r[6] else None,
                }
                for r in bloat
            ]
            bloat_rows = [
                [
                    r[0], r[1],
                    f"{int(r[2]):,}", f"{int(r[3]):,}",
                    f"{float(r[4]):.1f}%",
                    str(r[5]).split(".")[0] if r[5] else "-",
                    str(r[6]).split(".")[0] if r[6] else "-",
                ]
                for r in bloat
            ]
            print_table(
                ["Schema", "Table", "Dead Tuples", "Live Tuples", "Bloat %", "Last Autovacuum", "Last Vacuum"],
                bloat_rows,
            )
            print(f"  {'─' * 40}")
            print(f"  Total dead tuples: {total_dead:,} across {len(bloat)} tables")
        else:
            print("  (no dead tuples found)")

    @staticmethod
    def _pretty_bytes(b: int) -> str:
        for unit in ("bytes", "kB", "MB", "GB", "TB"):
            if abs(b) < 1024:
                return f"{b:.1f} {unit}" if unit != "bytes" else f"{b} {unit}"
            b /= 1024.0
        return f"{b:.1f} PB"

    # ── main ─────────────────────────────────────────────

    def run(self):
        try:
            if self.sizes_only:
                print(self._c(Colors.BOLD_BLUE, "XRAY PostgreSQL DB Sizes"))
            else:
                if self.use_exact_counts:
                    print(
                        self._c(Colors.BOLD_BLUE, "XRAY PostgreSQL DB Config [exact]")
                    )
                else:
                    print(
                        self._c(
                            Colors.BOLD_BLUE, "XRAY PostgreSQL DB Config [estimates]"
                        )
                        + self._c(
                            Colors.MAGENTA,
                            "  (use --exact-counts for precise measurements)",
                        )
                    )

            if not self.load_xray_environment():
                sys.exit(1)

            if self.sizes_only:
                self.show_sizes()
            else:
                self.check_service()
                self.check_database_exists()
                if self.verbose:
                    self.show_database_info()
                actual_ts = self.show_tablespace()
                self.show_tablespace_objects()
                if self.verbose:
                    self.show_pg_settings()
                elif self.output_file:
                    self.collect_pg_settings()
                self.show_top_tables_and_indexes()

            if self.output_file and "pg_settings" not in self.json_data:
                self.collect_pg_settings()

            if self.output_file:
                with open(self.output_file, "w") as f:
                    json.dump(self.json_data, f, indent=2)
                print()
                print(self._c(Colors.GREEN, f"  ✓ JSON written to {self.output_file}"))

            print()
            print(self._c(Colors.GREEN, "  ✓ Done"))
        finally:
            self.close_connections()


def main():
    parser = argparse.ArgumentParser(
        description="XRAY PostgreSQL Database Configuration Analyzer (Remote)",
    )
    parser.add_argument(
        "--exact-counts",
        "-e",
        action="store_true",
        help="Use exact calculations instead of estimates",
    )
    parser.add_argument(
        "--size",
        "-s",
        action="store_true",
        help="Show only size info: database, table, and index sizes with row counts",
    )
    parser.add_argument(
        "--verbose",
        "-V",
        action="store_true",
        help="Show extra detail (connection info, service status, PG version)",
    )
    parser.add_argument(
        "--env-file",
        default="./xray_env",
        help="Path to XRAY environment file (default: ./xray_env)",
    )
    parser.add_argument(
        "--output",
        "-o",
        metavar="FILE",
        help="Write output to a JSON file",
    )
    parser.add_argument(
        "--version",
        "-v",
        action="version",
        version="XRAY DB Config Analyzer v3.0 (Remote)",
    )
    args = parser.parse_args()

    try:
        checker = XrayDBConfig(
            use_exact_counts=args.exact_counts, xray_env_file=args.env_file
        )
        checker.sizes_only = args.size
        checker.verbose = args.verbose
        checker.output_file = args.output
        checker.run()
    except KeyboardInterrupt:
        print("\nCancelled.")
        sys.exit(1)
    except Exception as e:
        print(f"\nError: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
