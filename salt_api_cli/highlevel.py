"""High-level, human-readable commands for salt-api-cli.

The low-level commands (``local`` / ``runner`` / ``wheel``) are thin
passthroughs that dump raw salt-api JSON. The commands here are the
opposite: each knows the *shape* of a specific salt workflow and renders it
with :mod:`rich` for a human at a terminal, layered over the low-level
client in :mod:`salt_api_cli.lowlevel`.

* ``run_state`` — the ``salt state`` command (``highstate`` / ``apply`` /
  ``test``). It drives the ``local`` client with a ``state.*`` function and
  renders a coloured table of states with a summary, instead of the wall of
  JSON the raw ``local`` command would emit.
* ``run_keys`` — the ``salt keys`` command, layered over ``wheel key.*``.
  ``keys list`` shows one coloured panel per acceptance status (Accepted /
  Pending / Denied / Rejected).

Each command receives an injected ``call`` callable (bound to the right
client in cli.py), so this module never owns transport details. Colour and
box-drawing are handled by ``rich.Console``, which auto-disables them when
output is piped to a file or pager.
"""

from __future__ import annotations

import argparse
import json
import sys
from typing import Any, Callable, cast

from rich.columns import Columns
from rich.console import Console
from rich.padding import Padding
from rich.panel import Panel
from rich.table import Table
from rich.text import Text

from salt_api_cli.lowlevel import split_args

console = Console()

# (ASCII marker, rich style) for each per-state status. ASCII markers stay
# legible on any console; rich supplies the colour.
_STATUS_STYLE = {
    "ok": ("+", "green"),  # ran, no changes
    "change": ("*", "green"),  # ran, made changes
    "diff": ("~", "yellow"),  # test=True: would change
    "fail": ("X", "bold red"),  # failed
    "skip": (".", "dim"),  # requisites unmet, not run
}

# wheel key.list_all groups minion IDs under these keys; each renders as a
# panel whose border colour signals the acceptance status.
_KEY_PANELS = {
    "minions": ("Accepted", "green"),
    "minions_pre": ("Pending", "yellow"),
    "minions_denied": ("Denied", "red"),
    "minions_rejected": ("Rejected", "red"),
}


# --------------------------------------------------------------------------
# state rendering
# --------------------------------------------------------------------------


def _is_state_return(val: Any) -> bool:
    """True if ``val`` is a state return: a non-empty dict whose every value
    is itself a dict carrying a ``result`` key (the per-state record shape)."""
    if not isinstance(val, dict) or not val:
        return False
    records = cast("dict[str, Any]", val)
    return all(isinstance(v, dict) and "result" in v for v in records.values())


def _state_status(state: dict[str, Any]) -> str:
    """Classify one state record into an _STATUS_STYLE key."""
    if state.get("__state_ran__") is False:
        return "skip"
    result = state.get("result")
    if result is False:
        return "fail"
    if result is None:
        return "diff"
    return "change" if state.get("changes") else "ok"


def _state_function(key: str) -> str:
    """Recover ``module.func`` from a state key like
    ``cmd_|-veyon-installed_|-<name>_|-run`` -> ``cmd.run``."""
    parts = key.split("_|-")
    if len(parts) >= 2 and parts[-1]:
        return f"{parts[0]}.{parts[-1]}"
    return parts[0]


def _short(text: str, limit: int = 100) -> str:
    """Collapse whitespace and truncate a comment to one tidy line."""
    flat = " ".join(str(text).split())
    return flat if len(flat) <= limit else flat[: limit - 3] + "..."


def _fmt_duration(ms: float) -> str:
    return f"{ms / 1000:.2f}s" if ms >= 1000 else f"{ms:.0f}ms"


def _print_state_return(minion: str, states: dict[str, Any]) -> None:
    """Render one minion's state run: header, a table of states, summary."""
    ordered = sorted(states.items(), key=lambda kv: kv[1].get("__run_num__", 1 << 30))

    table = Table(box=None, show_header=False, pad_edge=False)
    table.add_column("marker", no_wrap=True)
    table.add_column("function", style="cyan", no_wrap=True)
    table.add_column("ref", style="dim", no_wrap=True)
    table.add_column("detail", no_wrap=True, overflow="ellipsis")

    counts = {k: 0 for k in _STATUS_STYLE}
    total_ms = 0.0
    for key, state in ordered:
        status = _state_status(state)
        counts[status] += 1
        try:
            total_ms += float(state.get("duration", 0) or 0)
        except (TypeError, ValueError):
            pass
        marker, style = _STATUS_STYLE[status]
        ref = f"{state.get('__sls__', '?')}:{state.get('__id__', key)}"
        if status == "ok":
            detail: str | Text = ""
        elif status == "change":
            changed = ", ".join(state.get("changes", {})) or "(changes)"
            detail = f"changed: {_short(changed)}"
        elif status == "fail":
            detail = Text(_short(state.get("comment", ""), 240), style="red")
        else:  # diff / skip
            detail = _short(state.get("comment", ""))
        table.add_row(Text(marker, style=style), _state_function(key), ref, detail)

    console.print(Text(minion, style="bold"))
    console.print(Padding(table, (0, 0, 0, 2)))

    parts = [f"[green]{counts['ok']} ok[/]"]
    if counts["change"]:
        parts.append(f"[green]{counts['change']} changed[/]")
    if counts["diff"]:
        parts.append(f"[yellow]{counts['diff']} would-change[/]")
    if counts["skip"]:
        parts.append(f"[dim]{counts['skip']} skipped[/]")
    parts.append(
        f"[red]{counts['fail']} failed[/]"
        if counts["fail"]
        else f"{counts['fail']} failed"
    )
    console.print("  [dim]---[/]")
    console.print(f"  {'   '.join(parts)}   [dim]took {_fmt_duration(total_ms)}[/]")


def _print_state_result(result: dict[str, Any]) -> None:
    """Render a state return from the local client, one block per minion.

    Falls back to indented JSON for anything that isn't a state return — e.g.
    a render/compile error, where salt answers with a list of message lines."""
    ret_list: Any = result.get("return")
    if not ret_list:
        console.print_json(json.dumps(result))
        return
    ret: dict[str, Any] = ret_list[0]
    if not ret:
        console.print("(no minions responded)")
        return
    for minion in sorted(ret):
        val = ret[minion]
        if _is_state_return(val):
            _print_state_return(minion, val)
            continue
        console.print(Text(minion, style="bold"))
        if isinstance(val, list):
            for item in cast("list[Any]", val):
                console.print(Padding(Text(str(item)), (0, 0, 0, 2)))
        else:
            console.print(Padding(json.dumps(val, indent=2), (0, 0, 0, 2)))


def run_state(args: argparse.Namespace, call: Callable[..., dict[str, Any]]) -> None:
    """The ``salt state`` command, layered over the local client + ``state.*``.

    ``call(tgt=..., fun=..., ...)`` must invoke the local client and return
    its JSON (cli.py binds it to the local client). Any trailing ``key=value``
    args are forwarded as kwargs to the state function (e.g. ``test=True``)."""
    pos, kw = split_args(list(getattr(args, "args", None) or []))
    if args.action == "highstate":
        fun, arg = "state.highstate", pos
    elif args.action == "test":
        fun, arg = "state.highstate", pos
        kw["test"] = "True"
    else:  # apply <sls>
        fun, arg = "state.apply", [args.sls, *pos]

    payload: dict[str, Any] = {"tgt": args.target, "fun": fun, "arg": arg}
    if kw:
        payload["kwarg"] = kw
    _print_state_result(call(**payload))


# --------------------------------------------------------------------------
# key management
# --------------------------------------------------------------------------


def _print_key_panels(data: dict[str, Any]) -> None:
    """Render key.list_all as one panel per acceptance status."""
    panels: list[Panel] = []
    for status_key, (label, color) in _KEY_PANELS.items():
        keys: list[str] = data.get(status_key, [])
        body: Any = Text("\n".join(keys)) if keys else Text("(none)", style="dim")
        panels.append(
            Panel(
                body,
                title=f"{label} ({len(keys)})",
                title_align="left",
                border_style=color,
            )
        )
    console.print(Columns(panels, equal=True, expand=False))


def run_keys(args: argparse.Namespace, call: Callable[..., dict[str, Any]]) -> None:
    """The ``salt keys`` command, layered over ``wheel key.*``.

    ``call(fun=..., **kw)`` must invoke the wheel client and return its JSON
    (cli.py binds it to the wheel client)."""
    action: str = args.action
    if action == "list":
        result = call(fun="key.list_all")
        _print_key_panels(result["return"][0]["data"]["return"])
        return

    fun_map = {
        "accept": "key.accept",
        "accept-all": "key.accept",
        "reject": "key.reject",
        "delete": "key.delete",
    }
    match: str = "*" if action == "accept-all" else args.match
    result = call(fun=fun_map[action], match=match)
    data = result["return"][0]["data"]
    if not data.get("success"):
        sys.exit(f"failed: {data}")
    changed: dict[str, list[str]] = data.get("return", {})
    if not changed:
        console.print("(no keys changed)")
        return
    for status_key, ids in changed.items():
        label = _KEY_PANELS.get(status_key, (status_key, "white"))[0]
        joined = ", ".join(ids) if ids else "[dim](none)[/]"
        console.print(f"{label}: {joined}")
