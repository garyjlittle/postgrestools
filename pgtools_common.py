#!/usr/bin/env python3
"""
pgtools_common.py - shared helpers for the postgrestools Python tools.

Single source of truth for the environment variables, SSH invocation, and
terminal output conventions used across every tool in this repo, so they stay
consistent.  New tools should import from here rather than re-defining these.

Environment variables (all optional; every tool also has a matching CLI flag):

    REMOTE_USER      SSH user for the host probe            (-U / --ssh-user)
    REMOTE_SSH_KEY   SSH private key path                   (-i / --ssh-key)
    PGUSER           PostgreSQL user for protocol/psql      (--pg-user)
    PGDATABASE       PostgreSQL maintenance database        (--pg-database)
    PGPASSWORD       PostgreSQL password                    (--pg-password)
    PGOSUSER         postgres OS account to sudo to         (--pg-os-user)
    SUDO_PASSWORD    sudo password for --sudo               (--sudo-password)

Conventions:
    * Tools run on a controlling machine; the target hostname is the FINAL
      positional argument.
    * stdlib only - no third-party modules are available on the controllers.
"""

import argparse
import os
import shlex
import shutil
import subprocess
import sys
from typing import Dict, List, Optional

# ── environment variable names (the canonical list) ──────────────────────

ENV_SSH_USER = "REMOTE_USER"
ENV_SSH_KEY = "REMOTE_SSH_KEY"
ENV_PG_USER = "PGUSER"
ENV_PG_DATABASE = "PGDATABASE"
ENV_PG_PASSWORD = "PGPASSWORD"
ENV_PG_OS_USER = "PGOSUSER"
ENV_SUDO_PASSWORD = "SUDO_PASSWORD"

DEFAULT_PG_USER = "postgres"
DEFAULT_PG_DATABASE = "postgres"
DEFAULT_PG_OS_USER = "postgres"
DEFAULT_CONNECT_TIMEOUT = 8


def env(name: str, default: Optional[str] = None) -> Optional[str]:
    """Read one of the canonical environment variables."""
    return os.environ.get(name, default)


# ── terminal output ──────────────────────────────────────────────────────


class Colors:
    RED = "\033[31m"
    GREEN = "\033[32m"
    YELLOW = "\033[33m"
    CYAN = "\033[36m"
    BOLD = "\033[1m"
    DIM = "\033[2m"
    RESET = "\033[0m"
    _enabled = True

    @classmethod
    def disable(cls) -> None:
        cls._enabled = False


def c(color: str, text: str) -> str:
    return f"{color}{text}{Colors.RESET}" if Colors._enabled else text


def info(msg: str) -> None:
    print(c(Colors.CYAN, "  " + msg))


def ok(msg: str) -> None:
    print(c(Colors.GREEN, "  ✓ " + msg))


def warn(msg: str) -> None:
    print(c(Colors.YELLOW, "  ! " + msg))


def err(msg: str) -> None:
    print(c(Colors.RED, "  ✗ " + msg))


def step(msg: str) -> None:
    print(c(Colors.BOLD, "\n" + msg))


def hr(width: int = 64) -> str:
    return "─" * width


def tick(value: Optional[bool]) -> str:
    if value is True:
        return c(Colors.GREEN, "yes")
    if value is False:
        return c(Colors.RED, "no")
    return c(Colors.DIM, "unknown")


# ── ssh ──────────────────────────────────────────────────────────────────


class SSHError(Exception):
    pass


def build_ssh_cmd(host: str, user: Optional[str], key: Optional[str],
                  extra: List[str], timeout: int) -> List[str]:
    if not shutil.which("ssh"):
        raise SSHError("no ssh client on this machine")
    cmd = ["ssh", "-o", "BatchMode=yes", "-o", f"ConnectTimeout={int(timeout)}",
           "-o", "StrictHostKeyChecking=accept-new", "-n"]
    if key:
        cmd += ["-i", os.path.expanduser(key)]
    cmd += list(extra)
    cmd += [f"{user}@{host}" if user else host]
    return cmd


def run_ssh(host: str, script: str, user: Optional[str], key: Optional[str],
            extra: List[str], timeout: int, check: bool = True,
            stdin_data: Optional[str] = None) -> str:
    """Run a POSIX-sh script on a host over ssh; return stdout."""
    cmd = build_ssh_cmd(host, user, key, extra, timeout) + [script]
    try:
        proc = subprocess.run(cmd, capture_output=True, text=True,
                              input=stdin_data, timeout=timeout + 30)
    except subprocess.TimeoutExpired:
        raise SSHError(f"ssh to {host} timed out")
    except OSError as exc:
        raise SSHError(f"could not run ssh: {exc}")
    if check and proc.returncode != 0:
        tail = (proc.stderr or "").strip().splitlines()
        raise SSHError(tail[-1] if tail else f"ssh exited {proc.returncode}")
    return proc.stdout or ""


def parse_kv_lines(text: str) -> Dict[str, List[str]]:
    """Parse `key=value` lines into a dict of lists (repeated keys kept)."""
    out: Dict[str, List[str]] = {}
    for line in text.splitlines():
        line = line.strip()
        if not line or "=" not in line:
            continue
        k, v = line.split("=", 1)
        if v:
            out.setdefault(k.strip(), []).append(v.strip())
    return out


def shq(value: str) -> str:
    """Shell-quote a value for embedding in a remote script."""
    return shlex.quote(value)


# ── shared CLI arguments ─────────────────────────────────────────────────


def add_ssh_args(parser: argparse.ArgumentParser) -> None:
    """Standard SSH connection arguments (env-var backed)."""
    parser.add_argument("-U", "--ssh-user", default=env(ENV_SSH_USER),
                        help=f"SSH user (default: ${ENV_SSH_USER})")
    parser.add_argument("-i", "--ssh-key", default=env(ENV_SSH_KEY),
                        help=f"SSH private key (default: ${ENV_SSH_KEY})")
    parser.add_argument("-o", "--ssh-opt", action="append", default=[],
                        metavar="OPT",
                        help="Extra raw ssh option, repeatable (e.g. -o ProxyJump=bastion)")


def add_pg_args(parser: argparse.ArgumentParser, password: bool = True) -> None:
    """Standard PostgreSQL connection arguments (env-var backed)."""
    parser.add_argument("--pg-user", default=env(ENV_PG_USER, DEFAULT_PG_USER),
                        help=f"PostgreSQL user (default: ${ENV_PG_USER} or {DEFAULT_PG_USER})")
    parser.add_argument("--pg-database",
                        default=env(ENV_PG_DATABASE, DEFAULT_PG_DATABASE),
                        help=f"maintenance database (default: ${ENV_PG_DATABASE} or {DEFAULT_PG_DATABASE})")
    if password:
        parser.add_argument("--pg-password", default=env(ENV_PG_PASSWORD),
                            help=f"PostgreSQL password (default: ${ENV_PG_PASSWORD})")


def add_sudo_args(parser: argparse.ArgumentParser) -> None:
    """Standard sudo-to-postgres arguments (env-var backed)."""
    parser.add_argument("--pg-os-user", default=env(ENV_PG_OS_USER, DEFAULT_PG_OS_USER),
                        help=f"postgres OS account to sudo to (default: ${ENV_PG_OS_USER} or {DEFAULT_PG_OS_USER})")
    parser.add_argument("--sudo-password", default=env(ENV_SUDO_PASSWORD),
                        help=f"sudo password (default: ${ENV_SUDO_PASSWORD}; "
                             "prompted interactively if needed)")


def add_common_args(parser: argparse.ArgumentParser) -> None:
    """Standard output/behaviour arguments."""
    parser.add_argument("--no-color", action="store_true", help="Disable ANSI colour")
    parser.add_argument("--json", action="store_true",
                        help="Emit machine-readable JSON")


def finish_args(args: argparse.Namespace) -> None:
    """Apply the shared post-parse behaviour (colour handling)."""
    if getattr(args, "no_color", False) or not sys.stdout.isatty() \
            or getattr(args, "json", False):
        Colors.disable()
