"""salt-cli — thin Python CLI for salt-api.

Stdlib-only. Logs in once with PAM creds, caches the token in
~/.cache/salt-cli/token.json, then invokes the salt-api local/runner/
wheel clients over HTTPS. Token auto-refreshes when expired.

Configuration (later sources override earlier):
    1. ~/.saltclirc                       INI file, [salt-cli] section
    2. environment variables              SALT_API_URL, SALT_API_USER,
                                          SALT_API_PASS, SALT_API_INSECURE
    3. command-line flags                 --url, --user, --password,
                                          --insecure

Any `key=value` argument to local/runner/wheel is parsed as a kwarg to
the salt function. Anything else is positional.
"""

from __future__ import annotations

import argparse
import configparser
import json
import os
import ssl
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

CONFIG_FILE = Path.home() / ".saltclirc"
CONFIG_SECTION = "salt-cli"
TOKEN_FILE = Path.home() / ".cache" / "salt-cli" / "token.json"
USER_AGENT = "salt-cli/1.0 (Mozilla/5.0 compatible)"

# Wheel key.list_all groups minion IDs by acceptance state under these keys.
KEY_STATUS_LABELS = {
    "minions": "Accepted",
    "minions_pre": "Pending",
    "minions_denied": "Denied",
    "minions_rejected": "Rejected",
}


@dataclass
class Config:
    url: str
    user: str
    password: str
    insecure: bool


def _truthy(value: str) -> bool:
    return value.strip().lower() in ("1", "true", "yes", "on")


def _load_config(args: argparse.Namespace) -> Config:
    file_section: dict[str, str] = {}
    if CONFIG_FILE.exists():
        parser = configparser.ConfigParser()
        parser.read(CONFIG_FILE)
        if parser.has_section(CONFIG_SECTION):
            file_section = dict(parser.items(CONFIG_SECTION))

    url: str = args.url or os.environ.get("SALT_API_URL") or file_section.get("url", "")
    user: str = (
        args.user
        or os.environ.get("SALT_API_USER")
        or file_section.get("user", "salt_api")
    )
    password: str = (
        args.password
        or os.environ.get("SALT_API_PASS")
        or file_section.get("password", "")
    )
    insecure: bool = (
        args.insecure
        or os.environ.get("SALT_API_INSECURE") == "1"
        or _truthy(file_section.get("insecure", ""))
    )

    if not url:
        sys.exit(
            "salt-api URL not set (use --url, SALT_API_URL, or url= in ~/.saltclirc)"
        )
    if not password:
        sys.exit(
            "salt-api password not set "
            "(use --password, SALT_API_PASS, or password= in ~/.saltclirc)"
        )
    return Config(
        url=url.rstrip("/"),
        user=user,
        password=password,
        insecure=insecure,
    )


def _ssl_ctx(cfg: Config) -> ssl.SSLContext | None:
    if not cfg.insecure:
        return None
    ctx = ssl.create_default_context()
    ctx.check_hostname = False
    ctx.verify_mode = ssl.CERT_NONE
    return ctx


def _http(req: Request, cfg: Config) -> dict[str, Any]:
    try:
        with urlopen(req, context=_ssl_ctx(cfg), timeout=30) as resp:
            data: Any = json.loads(resp.read())
            return data
    except HTTPError as e:
        body = e.read().decode(errors="replace")
        sys.exit(f"salt-api {e.code} {e.reason}: {body}")
    except URLError as e:
        sys.exit(f"salt-api unreachable: {e.reason}")


def _login(cfg: Config) -> dict[str, Any]:
    body = urlencode(
        {"username": cfg.user, "password": cfg.password, "eauth": "pam"}
    ).encode()
    req = Request(
        f"{cfg.url}/login",
        data=body,
        headers={"Accept": "application/json", "User-Agent": USER_AGENT},
    )
    data = _http(req, cfg)
    info: dict[str, Any] = data["return"][0]
    if "token" not in info:
        sys.exit(f"login failed: {info}")
    return info


def _get_token(cfg: Config) -> str:
    if TOKEN_FILE.exists():
        try:
            cached: dict[str, Any] = json.loads(TOKEN_FILE.read_text())
            if cached.get("expire", 0) > time.time() + 60:
                return str(cached["token"])
        except (json.JSONDecodeError, OSError, AttributeError, TypeError):
            pass
    info = _login(cfg)
    TOKEN_FILE.parent.mkdir(parents=True, exist_ok=True)
    TOKEN_FILE.write_text(json.dumps(info))
    try:
        os.chmod(TOKEN_FILE, 0o600)
    except OSError:
        pass
    return str(info["token"])


def _call(cfg: Config, client: str, **kwargs: Any) -> dict[str, Any]:
    payload = [{"client": client, **kwargs}]
    req = Request(
        cfg.url,
        data=json.dumps(payload).encode(),
        headers={
            "Accept": "application/json",
            "Content-Type": "application/json",
            "X-Auth-Token": _get_token(cfg),
            "User-Agent": USER_AGENT,
        },
    )
    return _http(req, cfg)


def _split_args(args: list[str]) -> tuple[list[str], dict[str, str]]:
    """Split positional args from key=value kwargs."""
    pos: list[str] = []
    kw: dict[str, str] = {}
    for a in args:
        if "=" in a and not a.startswith("="):
            k, v = a.split("=", 1)
            if k.isidentifier():
                kw[k] = v
                continue
        pos.append(a)
    return pos, kw


def _print_local_result(result: dict[str, Any]) -> None:
    """One row per minion. JSON-encode anything that isn't a scalar so
    multi-value returns (e.g. cmd.run dicts) stay on one line."""
    ret_list: Any = result.get("return")
    if not ret_list:
        print(json.dumps(result, indent=2))
        return
    ret: dict[str, Any] = ret_list[0]
    if not ret:
        print("(no minions responded)")
        return
    width = max(len(m) for m in ret)
    for minion in sorted(ret):
        val = ret[minion]
        if isinstance(val, (str, int, float, bool)) or val is None:
            print(f"{minion:<{width}}  {val}")
        else:
            print(f"{minion:<{width}}  {json.dumps(val)}")


def _run_local(cfg: Config, args: argparse.Namespace) -> None:
    pos, kw = _split_args(list(args.args))
    payload: dict[str, Any] = {"tgt": args.target, "fun": args.function, "arg": pos}
    if kw:
        payload["kwarg"] = kw
    _print_local_result(_call(cfg, "local", **payload))


def _run_client(cfg: Config, client: str, args: argparse.Namespace) -> None:
    pos, kw = _split_args(list(args.args))
    payload: dict[str, Any] = {"fun": args.function, "arg": pos}
    if kw:
        payload["kwarg"] = kw
    print(json.dumps(_call(cfg, client, **payload), indent=2))


def _run_keys(cfg: Config, args: argparse.Namespace) -> None:
    action: str = args.action
    if action == "list":
        result = _call(cfg, "wheel", fun="key.list_all")
        data: dict[str, Any] = result["return"][0]["data"]["return"]
        for status_key, label in KEY_STATUS_LABELS.items():
            keys: list[str] = data.get(status_key, [])
            print(f"{label} ({len(keys)}):")
            for k in keys:
                print(f"  {k}")
            print()
        return

    fun_map = {
        "accept": "key.accept",
        "accept-all": "key.accept",
        "reject": "key.reject",
        "delete": "key.delete",
    }
    match: str = "*" if action == "accept-all" else args.match
    result = _call(cfg, "wheel", fun=fun_map[action], match=match)
    data = result["return"][0]["data"]
    if not data.get("success"):
        sys.exit(f"failed: {data}")
    changed: dict[str, list[str]] = data.get("return", {})
    if not changed:
        print("(no keys changed)")
        return
    for status_key, ids in changed.items():
        label = KEY_STATUS_LABELS.get(status_key, status_key)
        print(f"{label}: {', '.join(ids) if ids else '(none)'}")


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="salt-cli",
        description="Thin Python CLI for salt-api.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=(
            "examples:\n"
            "  salt-cli local '*' test.ping\n"
            "  salt-cli local 'bml*' cmd.run 'whoami'\n"
            "  salt-cli local 'bml1' cmd.run 'Get-Date' shell=powershell\n"
            "  salt-cli runner manage.status\n"
            "  salt-cli wheel key.list_all\n"
            "  salt-cli keys list\n"
            "  salt-cli keys accept '<id-or-glob>'\n"
            "  salt-cli keys accept-all\n"
        ),
    )
    parser.add_argument("--url", help="salt-api base URL")
    parser.add_argument("--user", help="PAM username")
    parser.add_argument("--password", help="PAM password")
    parser.add_argument(
        "--insecure",
        action="store_true",
        help="skip TLS certificate verification",
    )

    sub = parser.add_subparsers(dest="command", required=True)

    p_local = sub.add_parser("local", help="run a function on minions")
    p_local.add_argument("target", help="minion target (id or glob)")
    p_local.add_argument("function", help="salt function (e.g. test.ping)")
    p_local.add_argument(
        "args", nargs=argparse.REMAINDER, help="positional and key=value args"
    )

    p_runner = sub.add_parser("runner", help="invoke a master-side runner")
    p_runner.add_argument("function")
    p_runner.add_argument("args", nargs=argparse.REMAINDER)

    p_wheel = sub.add_parser("wheel", help="invoke a master-side wheel function")
    p_wheel.add_argument("function")
    p_wheel.add_argument("args", nargs=argparse.REMAINDER)

    p_keys = sub.add_parser("keys", help="manage minion keys")
    keys_sub = p_keys.add_subparsers(dest="action", required=True)
    keys_sub.add_parser("list", help="show keys grouped by status")
    p_accept = keys_sub.add_parser("accept", help="accept a key by id or glob")
    p_accept.add_argument("match")
    keys_sub.add_parser("accept-all", help="accept every pending key")
    p_reject = keys_sub.add_parser("reject", help="reject a key by id or glob")
    p_reject.add_argument("match")
    p_delete = keys_sub.add_parser("delete", help="delete a key by id or glob")
    p_delete.add_argument("match")

    return parser


def main() -> None:
    parser = _build_parser()
    args = parser.parse_args()
    cfg = _load_config(args)

    if args.command == "local":
        _run_local(cfg, args)
    elif args.command == "runner":
        _run_client(cfg, "runner", args)
    elif args.command == "wheel":
        _run_client(cfg, "wheel", args)
    elif args.command == "keys":
        _run_keys(cfg, args)


if __name__ == "__main__":
    main()
