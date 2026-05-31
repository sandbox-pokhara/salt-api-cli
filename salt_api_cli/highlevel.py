"""High-level, human-readable commands for salt-api-cli.

The low-level commands (``local`` / ``runner`` / ``wheel``) are thin
passthroughs that dump raw salt-api JSON. The commands here are the
opposite: each knows the *shape* of a specific salt workflow and renders it
with :mod:`rich` for a human at a terminal, layered over the low-level
client in :mod:`salt_api_cli.lowlevel`.

* ``run_state`` — the ``salt state`` command (``highstate`` / ``apply`` /
  ``test``). It fires the ``state.*`` job through the ``local_async`` client
  (which returns a job id immediately, dodging the proxy/gateway connection
  cap that kills a long synchronous highstate) and then polls the ``runner``
  ``jobs.lookup_jid`` for results, showing a progress bar as minions report
  back and rendering the coloured per-minion tables once the run completes —
  instead of the wall of JSON the raw ``local`` command would emit.
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
import re
import sys
import time
from typing import Any, Callable, cast

from rich.columns import Columns
from rich.console import Console, Group
from rich.live import Live
from rich.padding import Padding
from rich.panel import Panel
from rich.spinner import Spinner
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


def _count_states(states: dict[str, Any]) -> tuple[dict[str, int], float]:
    """Tally per-status counts and summed duration (ms) for one minion's run.

    Shared by the per-minion summary and the fleet-wide grand total."""
    counts = {k: 0 for k in _STATUS_STYLE}
    total_ms = 0.0
    for state in states.values():
        counts[_state_status(state)] += 1
        try:
            total_ms += float(state.get("duration", 0) or 0)
        except (TypeError, ValueError):
            pass
    return counts, total_ms


def _counts_str(counts: dict[str, int]) -> str:
    """The status tally as markup: ``N ok  N changed  N would-change
    N skipped  N failed``. ``ok`` and ``failed`` always show; the rest only
    when non-zero."""
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
    return "   ".join(parts)


def _summary_line(counts: dict[str, int], took: str) -> str:
    """:func:`_counts_str` with a trailing ``took Xs`` (a preformatted
    duration)."""
    return f"{_counts_str(counts)}   [dim]took {took}[/]"


def _grand_totals(returns: dict[str, Any]) -> tuple[dict[str, int], int]:
    """Sum state counts across every minion that produced a state return,
    plus the number of such minions."""
    totals = {k: 0 for k in _STATUS_STYLE}
    n = 0
    for val in returns.values():
        if not _is_state_return(val):
            continue
        n += 1
        counts, _ = _count_states(val)
        for k in totals:
            totals[k] += counts[k]
    return totals, n


def _print_state_return(minion: str, states: dict[str, Any]) -> None:
    """Render one minion's state run: header, a table of states, summary."""
    ordered = sorted(states.items(), key=lambda kv: kv[1].get("__run_num__", 1 << 30))

    rows: list[tuple[Text, str, str, str | Text]] = []
    for key, state in ordered:
        status = _state_status(state)
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
        rows.append((Text(marker, style=style), _state_function(key), ref, detail))

    # Pin the detail column to whatever width is left so rich shrinks *it*
    # (ellipsis) rather than collapsing the short marker/function/ref columns
    # to nothing on a narrow terminal. Width budget: 2-space left Padding +
    # 1-char marker + the natural function/ref widths + three 2-space column
    # gaps (pad_edge=False). Floor at 20 so detail never vanishes outright.
    fn_w = max((len(fn) for _, fn, _, _ in rows), default=8)
    ref_w = max((len(ref) for _, _, ref, _ in rows), default=8)
    nat_w = max(
        (len(d.plain if isinstance(d, Text) else d) for _, _, _, d in rows), default=0
    )
    detail_w = min(nat_w, max(20, console.width - 2 - 1 - fn_w - ref_w - 3 * 2))

    table = Table(box=None, show_header=False, pad_edge=False)
    table.add_column("marker", no_wrap=True)
    table.add_column("function", style="cyan", no_wrap=True)
    table.add_column("ref", style="dim", no_wrap=True)
    table.add_column("detail", no_wrap=True, overflow="ellipsis", width=detail_w)

    for row in rows:
        table.add_row(*row)

    counts, total_ms = _count_states(states)
    console.print(Text(minion, style="bold"))
    console.print(Padding(table, (0, 0, 0, 2)))
    console.print("  [dim]---[/]")
    console.print(f"  {_summary_line(counts, _fmt_duration(total_ms))}")


def _print_one_minion(minion: str, val: Any) -> None:
    """Render a single minion's return block.

    A state return gets the coloured table; anything else (a render/compile
    error, where salt answers with a list of message lines, or some other
    shape) falls back to its lines or indented JSON."""
    if _is_state_return(val):
        _print_state_return(minion, val)
        return
    console.print(Text(minion, style="bold"))
    if isinstance(val, list):
        for item in cast("list[Any]", val):
            console.print(Padding(Text(str(item)), (0, 0, 0, 2)))
    else:
        console.print(Padding(json.dumps(val, indent=2), (0, 0, 0, 2)))


def _print_state_result(result: dict[str, Any]) -> None:
    """Render a state return, one block per minion (all at once).

    Falls back to indented JSON for anything that isn't a state return."""
    ret_list: Any = result.get("return")
    if not ret_list:
        console.print_json(json.dumps(result))
        return
    ret: dict[str, Any] = ret_list[0]
    if not ret:
        console.print("(no minions responded)")
        return
    for minion in sorted(ret):
        _print_one_minion(minion, ret[minion])


# How often to poll jobs.lookup_jid, and how long to keep waiting overall
# before giving up on minions that never reported. Each poll is a fast,
# self-contained request, so the proxy/gateway connection cap never bites.
#
# We don't probe minion liveness (saltutil.find_job): an empty probe is
# ambiguous — a busy-but-alive Windows minion mid-highstate can simply fail to
# answer in time and look identical to a down one, so probing wrongly dropped
# live minions. Instead we just poll until every targeted minion has returned
# or _POLL_DEADLINE trips, then render whatever came back. The job keeps
# running on the minions regardless; results stay fetchable later by jid. Press
# Ctrl+C to stop waiting early and render the partial results gathered so far.
_POLL_INTERVAL = 3.0
_POLL_DEADLINE = 1800.0  # 30 minutes (hard backstop)


def _first_return(resp: dict[str, Any]) -> Any:
    """The first element of a salt-api ``return`` list, or ``{}`` if absent."""
    ret = resp.get("return")
    if isinstance(ret, list) and ret:
        return cast("Any", ret[0])
    return {}


def _lookup_returns(raw: Any) -> dict[str, Any]:
    """Pull the ``{minion: state_return}`` map out of a jobs.lookup_jid reply.

    Over salt-api the runner wraps results in a display envelope —
    ``{"outputter": "highstate", "data": {minion: ...}}`` — unlike the bare
    ``{minion: ...}`` the local client returns. Unwrap ``data`` when present,
    and tolerate either shape (or junk) without raising."""
    if not isinstance(raw, dict):
        return {}
    data = cast("dict[str, Any]", raw)
    inner = data.get("data")
    return cast("dict[str, Any]", inner) if isinstance(inner, dict) else data


def _count_cells(counts: dict[str, int]) -> list[Text]:
    """One right-padded cell per status category, for column alignment in the
    live view. ``ok``/``failed`` always render; the rest blank when zero so
    the column still reserves its width and rows stay aligned."""
    blank = Text("")
    return [
        Text.from_markup(f"[green]{counts['ok']:>2} ok[/]"),
        Text.from_markup(f"[green]{counts['change']:>2} changed[/]")
        if counts["change"]
        else blank,
        Text.from_markup(f"[yellow]{counts['diff']:>2} would-change[/]")
        if counts["diff"]
        else blank,
        Text.from_markup(f"[dim]{counts['skip']:>2} skipped[/]")
        if counts["skip"]
        else blank,
        Text.from_markup(
            f"[red]{counts['fail']:>2} failed[/]"
            if counts["fail"]
            else f"[dim]{counts['fail']:>2} failed[/]"
        ),
    ]


def _state_cells(val: Any) -> list[Text]:
    """The five live-view columns for a finished minion's state return: its
    per-status tally, or a placeholder (plus blanks) for a non-state reply."""
    if _is_state_return(val):
        counts, _ = _count_states(cast("dict[str, Any]", val))
        return _count_cells(counts)
    return [Text("(no state output)", style="dim"), *[Text("")] * 4]


def _live_view(
    targeted: list[str],
    returns: dict[str, Any],
    done: set[str],
    missing: set[str],
    spinner: Spinner,
    *,
    n_cells: int,
    cells_for: Callable[[Any], list[Text]],
) -> Group:
    """A live checklist: a tick for finished minions (with ``cells_for`` of
    their reply in aligned columns), a spinner for the ones still running, an x
    for those that never reported, under a one-line status header. ``missing``
    is only populated in the final frame (after the deadline or a Ctrl+C); while
    polling it's empty, so still-pending minions show a spinner. ``n_cells`` is
    how many trailing columns ``cells_for`` produces (so blank rows stay
    aligned)."""
    blanks = [Text("")] * n_cells
    grid = Table.grid(padding=(0, 1))
    grid.add_column(no_wrap=True)  # marker
    grid.add_column(no_wrap=True)  # minion id
    for _ in range(n_cells):  # per-command trailing columns
        grid.add_column(no_wrap=True, justify="left")
    for minion in targeted:
        if minion in missing:
            grid.add_row(Text("X", style="red"), Text(minion, style="dim"), *blanks)
        elif minion in done:
            grid.add_row(
                Text("+", style="green"), Text(minion), *cells_for(returns.get(minion))
            )
        else:
            grid.add_row(spinner, Text(minion, style="dim"), *blanks)

    pending = len(targeted) - len(done) - len(missing)
    bits = [f"{len(done)}/{len(targeted)} done"]
    if pending:
        bits.append(f"{pending} running")
    if missing:
        bits.append(f"[red]{len(missing)} no response[/]")
    header = Text.from_markup(f"[dim]{'  '.join(bits)}[/]")
    return Group(header, grid)


def _stream_job(
    call: Callable[..., dict[str, Any]],
    payload: dict[str, Any],
    *,
    n_cells: int,
    cells_for: Callable[[Any], list[Text]],
) -> tuple[dict[str, Any], set[str], float, bool] | None:
    """Fire a job async, show a live checklist, and return its raw results.

    Submits ``payload`` via the ``local_async`` client (returns a job id at
    once), then polls ``runner jobs.lookup_jid`` until every targeted minion
    has returned, the deadline trips, or the user hits Ctrl+C. While polling it
    shows a live per-minion checklist (spinner -> tick), whose trailing columns
    come from ``cells_for(value)`` (``n_cells`` of them). In every case it then
    renders the final checklist frame and returns ``(returns, outstanding,
    start, interrupted)`` — ``outstanding`` being the targeted minions that
    never reported — for the caller to render, or ``None`` if no job started
    (already reported). ``call(name, **kw)`` invokes the named salt-api
    client."""
    submit = call("local_async", **payload)
    info: Any = _first_return(submit)
    jid = info.get("jid")
    if not jid:
        # No job id: either nothing matched (salt-api hands back an empty
        # body, e.g. {"return": [{}]}) or it answered with an error body. An
        # empty info means no minions matched — say so plainly; reserve the
        # raw JSON dump for an actual error worth showing verbatim.
        if not info:
            console.print("(no minions matched the target)")
        else:
            console.print_json(json.dumps(submit))
        return None

    targeted = sorted(info.get("minions") or [], key=_natural_key)
    if not targeted:
        console.print("(no minions matched the target)")
        return None

    expected = set(targeted)
    console.print(f"[dim]job {jid} -> {len(targeted)} minion(s)[/]")
    start = time.monotonic()
    returns: dict[str, Any] = {}
    spinner = Spinner("dots", style="cyan")

    def view(missing: set[str] | None = None) -> Group:
        done = expected & set(returns)
        return _live_view(
            targeted,
            returns,
            done,
            missing or set(),
            spinner,
            n_cells=n_cells,
            cells_for=cells_for,
        )

    # Poll lookup_jid until everyone's back or the deadline trips; Ctrl+C stops
    # waiting early. The job keeps running on the minions either way — we just
    # stop watching and render whatever was gathered. transient=False keeps the
    # finished checklist on screen above the rendered tables.
    interrupted = False
    with Live(console=console, refresh_per_second=12, transient=False) as live:
        try:
            while True:
                # lookup_jid is cumulative: each poll returns every minion that
                # has reported so far, so we just keep the latest snapshot.
                returns = _lookup_returns(
                    _first_return(
                        call("runner", fun="jobs.lookup_jid", kwarg={"jid": jid})
                    )
                )
                live.update(view())
                if not expected - set(returns):
                    break
                if time.monotonic() - start > _POLL_DEADLINE:
                    break
                time.sleep(_POLL_INTERVAL)
        except KeyboardInterrupt:
            interrupted = True
        # Final frame: mark whoever never reported so the persisted checklist
        # reflects the true end state rather than a frozen spinner.
        outstanding = expected - set(returns)
        live.update(view(outstanding))

    return returns, expected - set(returns), start, interrupted


def _print_outstanding(outstanding: set[str], interrupted: bool) -> None:
    """Trailer naming the minions that hadn't reported when we stopped waiting
    — because the user interrupted, or the deadline tripped."""
    if not outstanding:
        return
    names = ", ".join(sorted(outstanding, key=_natural_key))
    if interrupted:
        console.print(
            f"[yellow]stopped waiting (Ctrl+C); no result yet from: {names} "
            f"- the job may still be running on them[/]"
        )
    else:
        console.print(
            f"[yellow]no result from: {names} within the "
            f"{int(_POLL_DEADLINE)}s deadline (still running, or down)[/]"
        )


def _stream_state(call: Callable[..., dict[str, Any]], payload: dict[str, Any]) -> None:
    """Stream a state job, then render the coloured per-minion tables and a
    fleet-wide summary."""
    result = _stream_job(call, payload, n_cells=5, cells_for=_state_cells)
    if result is None:
        return
    returns, outstanding, start, interrupted = result

    # Live view cleared — render the coloured tables, one block per minion.
    _print_state_result({"return": [returns]})
    _print_outstanding(outstanding, interrupted)

    # Fleet-wide summary: totals across all minions + wall-clock elapsed.
    totals, n = _grand_totals(returns)
    if n:
        wall = _fmt_duration((time.monotonic() - start) * 1000.0)
        console.print("[dim]===[/]")
        console.print(f"[bold]{n} minion(s)[/]   {_summary_line(totals, wall)}")


def run_state(args: argparse.Namespace, call: Callable[..., dict[str, Any]]) -> None:
    """The ``salt state`` command, layered over ``local_async`` + ``state.*``.

    ``call(name, **kw)`` must invoke the named salt-api client and return its
    JSON (cli.py binds it to the configured connection). The job is fired
    async and its results streamed minion-by-minion via the runner. Any
    trailing ``key=value`` args are forwarded as kwargs to the state function
    (e.g. ``test=True``)."""
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
    _stream_state(call, payload)


# --------------------------------------------------------------------------
# key management
# --------------------------------------------------------------------------


def _natural_key(name: str) -> list[object]:
    """Sort key that orders embedded numbers numerically (bml2 before bml10)."""
    return [int(p) if p.isdigit() else p for p in re.split(r"(\d+)", name)]


def _print_key_panels(data: dict[str, Any]) -> None:
    """Render key.list_all as one stacked panel per acceptance status, the
    IDs flowed into aligned columns inside each panel."""
    for status_key, (label, color) in _KEY_PANELS.items():
        keys: list[str] = sorted(data.get(status_key, []), key=_natural_key)
        body: Any = (
            Columns([Text(k) for k in keys], padding=(0, 2))
            if keys
            else Text("(none)", style="dim")
        )
        console.print(
            Panel(
                body,
                title=f"{label} ({len(keys)})",
                title_align="left",
                border_style=color,
            )
        )


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


# --------------------------------------------------------------------------
# command execution
# --------------------------------------------------------------------------


def _print_cmd_one(minion: str, val: Any) -> None:
    """Render one minion's ``cmd.run_all`` reply: a bold id with its exit code
    (green for 0, red otherwise), then stdout and any stderr indented beneath.

    Falls back to printing the raw value for any non-dict shape — e.g. a
    minion that errored before the command ran, where salt returns a string."""
    if not isinstance(val, dict):
        console.print(Text(minion, style="bold"))
        console.print(Padding(Text(str(val)), (0, 0, 0, 2)))
        return

    record = cast("dict[str, Any]", val)
    retcode = record.get("retcode")
    header = Text(minion, style="bold")
    if retcode == 0:
        header.append("  exit 0", style="green")
    elif retcode is not None:
        header.append(f"  exit {retcode}", style="red")
    console.print(header)

    stdout = str(record.get("stdout", "")).rstrip()
    stderr = str(record.get("stderr", "")).rstrip()
    if stdout:
        console.print(Padding(Text(stdout), (0, 0, 0, 2)))
    if stderr:
        console.print(Padding(Text("stderr:", style="red"), (0, 0, 0, 2)))
        console.print(Padding(Text(stderr, style="red"), (0, 0, 0, 4)))
    if not stdout and not stderr:
        console.print(Padding(Text("(no output)", style="dim"), (0, 0, 0, 2)))


def _print_cmd_result(resp: dict[str, Any]) -> None:
    """Render a ``cmd.run_all`` reply, one block per minion (naturally sorted)."""
    ret = _first_return(resp)
    if not isinstance(ret, dict) or not ret:
        console.print("(no minions responded)")
        return
    results = cast("dict[str, Any]", ret)
    for minion in sorted(results, key=_natural_key):
        _print_cmd_one(minion, results[minion])


def _cmd_cells(val: Any) -> list[Text]:
    """The single live-view column for a finished minion's ``cmd.run_all``
    reply: its exit code, green for 0 and red otherwise."""
    if isinstance(val, dict):
        retcode = cast("dict[str, Any]", val).get("retcode")
        if retcode == 0:
            return [Text("exit 0", style="green")]
        if retcode is not None:
            return [Text(f"exit {retcode}", style="red")]
    return [Text("(no output)", style="dim")]


def _stream_cmd(call: Callable[..., dict[str, Any]], payload: dict[str, Any]) -> None:
    """Stream a ``cmd.run_all`` job, then render each minion's output block and
    a fleet-wide ok/failed summary."""
    result = _stream_job(call, payload, n_cells=1, cells_for=_cmd_cells)
    if result is None:
        return
    returns, outstanding, start, interrupted = result

    _print_cmd_result({"return": [returns]})
    _print_outstanding(outstanding, interrupted)

    n = len(returns)
    if n:
        ok = sum(
            1
            for v in returns.values()
            if isinstance(v, dict) and cast("dict[str, Any]", v).get("retcode") == 0
        )
        fail = n - ok
        wall = _fmt_duration((time.monotonic() - start) * 1000.0)
        tally = f"[green]{ok} ok[/]   " + (
            f"[red]{fail} failed[/]" if fail else f"{fail} failed"
        )
        console.print("[dim]===[/]")
        console.print(f"[bold]{n} minion(s)[/]   {tally}   [dim]took {wall}[/]")


def run_cmd(args: argparse.Namespace, call: Callable[..., dict[str, Any]]) -> None:
    """The ``salt cmd`` command, layered over ``local_async`` + ``cmd.run_all``.

    Runs a shell command on the targeted minions and streams the results: a
    live per-minion checklist (spinner -> exit code) while the job runs, then a
    readable block per minion (exit code, stdout, stderr) and an ok/failed
    summary — instead of the raw JSON the low-level ``local`` command emits.
    Like ``state``, it fires the job async and polls the runner so a slow or
    wide command never holds one long connection open against the gateway cap.
    Trailing ``key=value`` args are forwarded as kwargs to ``cmd.run_all``
    (e.g. ``shell=powershell``, ``cwd=...``, ``runas=...``). ``call(name,
    **kw)`` invokes the named salt-api client (cli.py binds it to the
    configured connection)."""
    pos, kw = split_args(list(getattr(args, "args", None) or []))
    payload: dict[str, Any] = {
        "tgt": args.target,
        "fun": "cmd.run_all",
        "arg": [args.cmdline, *pos],
    }
    if kw:
        payload["kwarg"] = kw
    _stream_cmd(call, payload)
