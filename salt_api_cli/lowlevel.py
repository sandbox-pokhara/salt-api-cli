"""Low-level salt-api transport for salt-api-cli.

This is the thin client layer: configuration loading, PAM login, token
caching/self-healing, and the raw ``_call`` that POSTs a client request to
salt-api and returns its decoded JSON. It knows nothing about how results
are rendered — that lives in :mod:`salt_api_cli.highlevel`.

The cached token self-heals: it is refreshed proactively when its stored
expiry has passed, and reactively when the server rejects it (HTTP 401 or
an EAUTH body) — e.g. after the salt-master container restarts and wipes
its session store. On rejection the CLI discards the token, logs in again,
and retries the request once before giving up.

Configuration (later sources override earlier):
    1. ~/.saltapiclirc                    INI file, [salt-api-cli] section
    2. environment variables              SALT_API_URL, SALT_API_USER,
                                          SALT_API_PASS, SALT_API_INSECURE
    3. command-line flags                 --url, --user, --password,
                                          --insecure, --relogin,
                                          --no-token-cache
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


def load_config(args: argparse.Namespace) -> Config:
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


def clear_token() -> None:
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


def call(cfg: Config, client: str, **kwargs: Any) -> dict[str, Any]:
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
    clear_token()
    try:
        result = attempt(_fresh_token(cfg))
    except AuthError as e:
        raise AuthError(f"{_AUTH_FAIL_HINT}\ndetails: {e}") from e
    if _is_auth_failure(result):
        raise AuthError(_AUTH_FAIL_HINT)
    return result


def split_args(args: list[str]) -> tuple[list[str], dict[str, str]]:
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
