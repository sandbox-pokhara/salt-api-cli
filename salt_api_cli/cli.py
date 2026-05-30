"""salt-api-cli — thin Python CLI for salt-api.

Logs in once with PAM creds, caches the token in
~/.cache/salt-api-cli/token.json, then invokes the salt-api local/
runner/wheel clients over HTTPS. Depends only on the stdlib plus
``typeguard`` for validating cached/responded JSON.

This module is the CLI glue: it parses arguments and dispatches each
command, wiring the low-level transport (:mod:`salt_api_cli.lowlevel`) to
the high-level human-readable rendering (:mod:`salt_api_cli.highlevel`).

The cached token self-heals: it is refreshed proactively when its stored
expiry has passed, and reactively when the server rejects it (HTTP 401 or
an EAUTH body) — e.g. after the salt-master container restarts and wipes
its session store. `--relogin` forces a fresh login, `--no-token-cache`
skips the cache entirely, and the `logout` subcommand discards the token.

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
import json
from typing import Any

from salt_api_cli import highlevel
from salt_api_cli.lowlevel import (
    TOKEN_FILE,
    Config,
    SaltApiError,
    call,
    clear_token,
    load_config,
    split_args,
)


def _run_local(cfg: Config, args: argparse.Namespace) -> None:
    pos, kw = split_args(list(args.args))
    payload: dict[str, Any] = {"tgt": args.target, "fun": args.function, "arg": pos}
    if kw:
        payload["kwarg"] = kw
    print(json.dumps(call(cfg, "local", **payload), indent=2))


def _run_client(cfg: Config, client: str, args: argparse.Namespace) -> None:
    pos, kw = split_args(list(args.args))
    payload: dict[str, Any] = {"fun": args.function, "arg": pos}
    if kw:
        payload["kwarg"] = kw
    print(json.dumps(call(cfg, client, **payload), indent=2))


def _run_state(cfg: Config, args: argparse.Namespace) -> None:
    def client(name: str, **kw: Any) -> dict[str, Any]:
        return call(cfg, name, **kw)

    highlevel.run_state(args, client)


def _run_keys(cfg: Config, args: argparse.Namespace) -> None:
    def wheel(**kw: Any) -> dict[str, Any]:
        return call(cfg, "wheel", **kw)

    highlevel.run_keys(args, wheel)


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="salt",
        description="Thin Python CLI for salt-api.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=(
            "low-level (raw JSON):\n"
            "  salt local '*' test.ping\n"
            "  salt local 'bml*' cmd.run 'whoami'\n"
            "  salt local 'bml1' cmd.run 'Get-Date' shell=powershell\n"
            "  salt runner manage.status\n"
            "  salt wheel key.list_all\n"
            "high-level (readable):\n"
            "  salt state highstate 'bml1'\n"
            "  salt state test 'bml1'              # dry-run highstate (test=True)\n"
            "  salt state apply 'bml1' veyon\n"
            "  salt keys list\n"
            "  salt keys accept '<id-or-glob>'\n"
            "  salt keys accept-all\n"
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

    p_state = sub.add_parser("state", help="apply states with readable output")
    state_sub = p_state.add_subparsers(dest="action", required=True)
    p_highstate = state_sub.add_parser("highstate", help="apply the highstate")
    p_highstate.add_argument("target", help="minion target (id or glob)")
    p_highstate.add_argument(
        "args", nargs=argparse.REMAINDER, help="key=value args, e.g. test=True"
    )
    p_test = state_sub.add_parser(
        "test", help="dry-run the highstate (forces test=True)"
    )
    p_test.add_argument("target", help="minion target (id or glob)")
    p_test.add_argument("args", nargs=argparse.REMAINDER, help="extra key=value args")
    p_apply = state_sub.add_parser("apply", help="apply specific sls module(s)")
    p_apply.add_argument("target", help="minion target (id or glob)")
    p_apply.add_argument("sls", help="sls module to apply (e.g. veyon or veyon.ldap)")
    p_apply.add_argument(
        "args", nargs=argparse.REMAINDER, help="key=value args, e.g. test=True"
    )

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
        clear_token()
        print(
            f"discarded cached token ({TOKEN_FILE})"
            if existed
            else f"no cached token to discard ({TOKEN_FILE})"
        )
        return

    cfg = load_config(args)

    try:
        if args.command == "local":
            _run_local(cfg, args)
        elif args.command == "runner":
            _run_client(cfg, "runner", args)
        elif args.command == "wheel":
            _run_client(cfg, "wheel", args)
        elif args.command == "state":
            _run_state(cfg, args)
        elif args.command == "keys":
            _run_keys(cfg, args)
    except SaltApiError as e:
        raise SystemExit(str(e))


if __name__ == "__main__":
    main()
