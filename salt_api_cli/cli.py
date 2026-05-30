"""salt-api-cli — thin Python CLI for salt-api.

Logs in once with PAM creds, caches the token in
~/.cache/salt-api-cli/token.json, then invokes the salt-api local/
runner/wheel clients over HTTPS. Depends only on the stdlib plus
``typeguard`` for validating cached/responded JSON.

The cached token self-heals: it is refreshed proactively when its stored
expiry has passed, and reactively when the server rejects it (HTTP 401 or
an EAUTH body) — e.g. after the salt-master container restarts and wipes
its session store. On rejection the CLI discards the token, logs in again,
and retries the request once before giving up. `--relogin` forces a fresh
login, `--no-token-cache` skips the cache entirely, and the `logout`
subcommand discards the cached token.

Configuration (later sources override earlier):
    1. ~/.saltapiclirc                    INI file, [salt-api-cli] section
    2. environment variables              SALT_API_URL, SALT_API_USER,
                                          SALT_API_PASS, SALT_API_INSECURE
    3. command-line flags                 --url, --user, --password,
                                          --insecure, --relogin,
                                          --no-token-cache

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

from typeguard import TypeCheckError, check_type

CONFIG_FILE = Path.home() / ".saltapiclirc"
CONFIG_SECTION = "salt-api-cli"
TOKEN_FILE = Path.home() / ".cache" / "salt-api-cli" / "token.json"
USER_AGENT = "salt-api-cli/1.0 (Mozilla/5.0 compatible)"

# Treat a cached token as already gone this many seconds before its real
# expiry, so we never send one that lapses mid-flight.
TOKEN_EXPIRY_MARGIN = 60

# Substrings that mark a salt-api JSON body as an auth failure. salt-api
# usually answers an invalid token with HTTP 401, but it sometimes returns
# 200 with one of these in the payload instead.
_AUTH_FAIL_MARKERS = (
    "eauth",
    "no permission",
    "not authorized",
    "authentication denied",
    "failed to authenticate",
)

_AUTH_FAIL_HINT = (
    "salt-api authentication still failed after a fresh login — the "
    "credentials may be wrong or the user lacks permission "
    "(check --user/--password, SALT_API_USER/SALT_API_PASS, or "
    "~/.saltapiclirc)."
)


class SaltApiError(Exception):
    """A salt-api error whose message is safe to show the user verbatim."""


class AuthError(SaltApiError):
    """An authentication failure (HTTP 401 or an EAUTH/auth-failure body).

    Signals that the token in hand was rejected and a re-login should be
    attempted.
    """

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
    # Ignore any cached token and log in fresh (the new token is still cached).
    relogin: bool = False
    # Neither read nor write the token cache for this run.
    no_token_cache: bool = False


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
            "salt-api URL not set (use --url, SALT_API_URL, or url= in ~/.saltapiclirc)"
        )
    if not password:
        sys.exit(
            "salt-api password not set "
            "(use --password, SALT_API_PASS, or password= in ~/.saltapiclirc)"
        )
    return Config(
        url=url.rstrip("/"),
        user=user,
        password=password,
        insecure=insecure,
        relogin=bool(getattr(args, "relogin", False)),
        no_token_cache=bool(getattr(args, "no_token_cache", False)),
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
        if e.code == 401:
            raise AuthError(f"salt-api 401 {e.reason}: {body}") from e
        raise SaltApiError(f"salt-api {e.code} {e.reason}: {body}") from e
    except URLError as e:
        raise SaltApiError(f"salt-api unreachable: {e.reason}") from e


def _is_auth_failure(result: dict[str, Any]) -> bool:
    """True if a 200 response body is actually an EAUTH/auth-failure notice."""
    texts: list[str] = []
    try:
        texts.extend(check_type(result.get("return"), list[str]))
    except TypeCheckError:
        pass  # a normal result ("return" is a list of dicts) — not an auth body
    for key in ("error", "status"):
        val = result.get(key)
        if isinstance(val, str):
            texts.append(val)
    return any(m in t.lower() for t in texts for m in _AUTH_FAIL_MARKERS)


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


def _read_cached_token(cfg: Config) -> str | None:
    """Return a still-valid cached token, or None to force a fresh login.

    Tolerant of a missing, empty, corrupt, or schema-mismatched token.json:
    any problem reading it is treated as "no usable token". A token whose
    `expire` is in the past (within a safety margin) is also discarded.
    """
    if cfg.relogin or cfg.no_token_cache:
        return None
    try:
        raw = TOKEN_FILE.read_text()
    except OSError:
        return None
    try:
        cached = check_type(json.loads(raw), dict[str, Any])
    except (json.JSONDecodeError, ValueError, TypeCheckError):
        return None
    token = cached.get("token")
    if not token:
        return None
    try:
        expire = float(cached.get("expire", 0))
    except (TypeError, ValueError):
        return None
    if expire <= time.time() + TOKEN_EXPIRY_MARGIN:
        return None
    return str(token)


def _clear_token() -> None:
    """Discard the cached token, if any. Never raises."""
    try:
        TOKEN_FILE.unlink()
    except OSError:
        pass


def _fresh_token(cfg: Config) -> str:
    """Log in and (unless caching is disabled) persist the new token."""
    info = _login(cfg)
    if not cfg.no_token_cache:
        try:
            TOKEN_FILE.parent.mkdir(parents=True, exist_ok=True)
            TOKEN_FILE.write_text(json.dumps(info))
            os.chmod(TOKEN_FILE, 0o600)
        except OSError:
            pass
    return str(info["token"])


def _get_token(cfg: Config) -> str:
    cached = _read_cached_token(cfg)
    if cached is not None:
        return cached
    return _fresh_token(cfg)


def _call(cfg: Config, client: str, **kwargs: Any) -> dict[str, Any]:
    payload = [{"client": client, **kwargs}]
    body = json.dumps(payload).encode()

    def attempt(token: str) -> dict[str, Any]:
        req = Request(
            cfg.url,
            data=body,
            headers={
                "Accept": "application/json",
                "Content-Type": "application/json",
                "X-Auth-Token": token,
                "User-Agent": USER_AGENT,
            },
        )
        return _http(req, cfg)

    # First try with whatever token we have (cached or freshly minted). A
    # rejected token here means it went stale server-side (expiry, or the
    # salt-master session store was wiped on restart) — not bad credentials.
    try:
        result = attempt(_get_token(cfg))
        if not _is_auth_failure(result):
            return result
    except AuthError:
        pass

    # Discard the stale token, log in fresh, and retry exactly once.
    _clear_token()
    try:
        result = attempt(_fresh_token(cfg))
    except AuthError as e:
        raise AuthError(f"{_AUTH_FAIL_HINT}\ndetails: {e}") from e
    if _is_auth_failure(result):
        raise AuthError(_AUTH_FAIL_HINT)
    return result


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
        prog="salt-api-cli",
        description="Thin Python CLI for salt-api.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=(
            "examples:\n"
            "  salt-api-cli local '*' test.ping\n"
            "  salt-api-cli local 'bml*' cmd.run 'whoami'\n"
            "  salt-api-cli local 'bml1' cmd.run 'Get-Date' shell=powershell\n"
            "  salt-api-cli runner manage.status\n"
            "  salt-api-cli wheel key.list_all\n"
            "  salt-api-cli keys list\n"
            "  salt-api-cli keys accept '<id-or-glob>'\n"
            "  salt-api-cli keys accept-all\n"
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
    parser.add_argument(
        "--relogin",
        action="store_true",
        help="ignore any cached token and log in fresh (re-caches the new token)",
    )
    parser.add_argument(
        "--no-token-cache",
        dest="no_token_cache",
        action="store_true",
        help="do not read or write the token cache for this run",
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

    sub.add_parser("logout", help="discard the cached auth token")

    return parser


def main() -> None:
    parser = _build_parser()
    args = parser.parse_args()

    # logout needs no server config — it just drops the local token file.
    if args.command == "logout":
        existed = TOKEN_FILE.exists()
        _clear_token()
        print(
            f"discarded cached token ({TOKEN_FILE})"
            if existed
            else f"no cached token to discard ({TOKEN_FILE})"
        )
        return

    cfg = _load_config(args)

    try:
        if args.command == "local":
            _run_local(cfg, args)
        elif args.command == "runner":
            _run_client(cfg, "runner", args)
        elif args.command == "wheel":
            _run_client(cfg, "wheel", args)
        elif args.command == "keys":
            _run_keys(cfg, args)
    except SaltApiError as e:
        sys.exit(str(e))


if __name__ == "__main__":
    main()
