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

from salt_api_cli.lowlevel import SaltApiError, split_args

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

    table = Table(box=None, show_header=False, pad_edge=False)
    table.add_column("marker", no_wrap=True)
    table.add_column("function", style="cyan", no_wrap=True)
    table.add_column("ref", style="dim", no_wrap=True)
    table.add_column("detail", no_wrap=True, overflow="ellipsis")

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
        table.add_row(Text(marker, style=style), _state_function(key), ref, detail)

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
_POLL_INTERVAL = 3.0
_POLL_DEADLINE = 1800.0  # 30 minutes (hard backstop)

# Once this many seconds pass with no new minion reporting, probe the
# still-outstanding minions with saltutil.find_job to tell "still running"
# apart from "down / lost the job" — the latter are dropped so we stop
# waiting on them.
#
# The probe passes BOTH a short publish ``timeout`` and a short
# ``gather_job_timeout``. The latter matters most: when a call targets an
# offline minion, the master runs its own internal find_job and waits
# gather_job_timeout (default ~10s on the master) for a reply that never
# comes — so without overriding it, flagging an offline minion costs ~10s+
# no matter how small ``timeout`` is. With both set low the cost drops to a
# few seconds (verified against this master: offline minion flagged in ~3s).
# find_job reports whether a minion is *running the job*, so an online minion
# answers within ``timeout`` and is never wrongly dropped. Instant detection
# would need presence_events on the master (manage.present/alived are empty).
_GATHER_TIMEOUT = 5.0
_FIND_JOB_TIMEOUT = 2.0
_FIND_JOB_GATHER = 2.0


def _first_return(resp: dict[str, Any]) -> Any:
    """The first element of a salt-api ``return`` list, or ``{}`` if absent."""
    ret = resp.get("return")
    if isinstance(ret, list) and ret:
        return cast("Any", ret[0])
    return {}


def _find_dead(
    call: Callable[..., dict[str, Any]], jid: str, candidates: set[str]
) -> set[str]:
    """Return the candidates that are NOT running ``jid`` (down or lost it).

    Probes only ``candidates`` via the local client + ``saltutil.find_job``
    with a short timeout. A minion actively running the job answers with a
    non-empty dict naming the jid; one that's down never answers, and one
    that's up but no longer running it answers empty — both mean it won't
    return, so it's reported dead. A failed probe reports nobody dead (we'd
    rather wait than wrongly drop a live minion)."""
    if not candidates:
        return set()
    try:
        resp = call(
            "local",
            tgt=sorted(candidates),
            tgt_type="list",
            fun="saltutil.find_job",
            arg=[jid],
            timeout=_FIND_JOB_TIMEOUT,
            gather_job_timeout=_FIND_JOB_GATHER,
        )
    except SaltApiError:
        return set()
    ret = _first_return(resp)
    if not isinstance(ret, dict):
        return set()
    running = cast("dict[str, Any]", ret)
    return {m for m in candidates if not running.get(m)}


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


def _live_view(
    targeted: list[str],
    returns: dict[str, Any],
    done: set[str],
    dead: set[str],
    spinner: Spinner,
) -> Group:
    """A live checklist: a tick for finished minions (with their per-state
    tally in aligned columns), a spinner for the ones still running, an x for
    the unreachable, under a one-line status header."""
    blanks = [Text("")] * 5  # the five count columns, empty
    grid = Table.grid(padding=(0, 1))
    grid.add_column(no_wrap=True)  # marker
    grid.add_column(no_wrap=True)  # minion id
    for _ in range(5):  # ok / changed / would-change / skipped / failed
        grid.add_column(no_wrap=True, justify="left")
    for minion in targeted:
        if minion in dead:
            grid.add_row(Text("✗", style="red"), Text(minion, style="dim"), *blanks)
        elif minion in done:
            val = returns.get(minion)
            if _is_state_return(val):
                counts, _ = _count_states(cast("dict[str, Any]", val))
                cells = _count_cells(counts)
            else:
                cells = [Text("(no state output)", style="dim"), *blanks[1:]]
            grid.add_row(Text("✓", style="green"), Text(minion), *cells)
        else:
            grid.add_row(spinner, Text(minion, style="dim"), *blanks)

    pending = len(targeted) - len(done) - len(dead)
    bits = [f"{len(done)}/{len(targeted)} done"]
    if pending:
        bits.append(f"{pending} running")
    if dead:
        bits.append(f"[red]{len(dead)} unreachable[/]")
    header = Text.from_markup(f"[dim]{'  '.join(bits)}[/]")
    return Group(header, grid)


def _stream_state(call: Callable[..., dict[str, Any]], payload: dict[str, Any]) -> None:
    """Fire a state job async, show a live checklist, then render the results.

    Submits via the ``local_async`` client (returns a job id at once), then
    polls ``runner jobs.lookup_jid`` until every targeted minion has returned
    or the deadline trips. While polling it shows a live per-minion checklist
    (spinner -> tick). Once the run is done the live view is cleared and the
    coloured per-minion tables print together, followed by a fleet-wide
    summary. ``call(name, **kw)`` invokes the named salt-api client."""
    submit = call("local_async", **payload)
    info: Any = _first_return(submit)
    jid = info.get("jid")
    if not jid:
        # No job id: nothing matched, or salt-api answered with an error body.
        console.print_json(json.dumps(submit))
        return

    targeted = sorted(info.get("minions") or [])
    if not targeted:
        console.print("(no minions matched the target)")
        return

    expected = set(targeted)  # shrinks as unreachable minions are dropped
    console.print(f"[dim]job {jid} -> {len(targeted)} minion(s)[/]")
    start = time.monotonic()
    returns: dict[str, Any] = {}
    dead: set[str] = set()  # probed and confirmed not running the job
    spinner = Spinner("dots", style="cyan")

    # transient=False keeps the finished checklist on screen above the
    # rendered tables, as a persistent at-a-glance record of the run.
    with Live(console=console, refresh_per_second=12, transient=False) as live:
        prev_done = -1
        last_change = start
        while True:
            # lookup_jid is cumulative: each poll returns every minion that has
            # reported so far, so we just keep the latest snapshot.
            returns = _lookup_returns(
                _first_return(call("runner", fun="jobs.lookup_jid", kwarg={"jid": jid}))
            )
            done = expected & set(returns)
            now = time.monotonic()
            if len(done) != prev_done:
                prev_done, last_change = len(done), now
            live.update(_live_view(targeted, returns, done, dead, spinner))

            if not expected - done:
                break

            # Stalled? Ask the stragglers whether they're still running the
            # job; drop the ones that aren't (down or lost it) so we stop
            # waiting on them instead of blocking to the deadline.
            if now - last_change > _GATHER_TIMEOUT:
                gone = _find_dead(call, jid, expected - done)
                if gone:
                    dead |= gone
                    expected -= gone
                    last_change = now  # don't re-probe every single poll
                    live.update(_live_view(targeted, returns, done, dead, spinner))
                    if not expected - done:
                        break

            if now - start > _POLL_DEADLINE:
                break
            time.sleep(_POLL_INTERVAL)

    # Live view cleared — render the coloured tables, one block per minion.
    _print_state_result({"return": [returns]})
    if dead:
        console.print(
            f"[yellow]no response from: {', '.join(sorted(dead))} "
            f"(down, or no longer running the job)[/]"
        )
    stalled = sorted(expected - set(returns) - dead)
    if stalled:
        console.print(
            f"[yellow]still running at the {int(_POLL_DEADLINE)}s deadline: "
            f"{', '.join(stalled)}[/]"
        )

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
