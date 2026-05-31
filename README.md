# salt-api-cli

Thin Python CLI for [salt-api](https://docs.saltproject.io/en/latest/ref/netapi/all/salt.netapi.rest_cherrypy.html).
Depends only on the standard library plus [`rich`](https://github.com/Textualize/rich)
(readable output) and [`typeguard`](https://github.com/agronholm/typeguard)
(JSON validation).

Logs in once with PAM credentials, caches the token in
`~/.cache/salt-api-cli/token.json`, then invokes salt-api's `local`,
`runner`, and `wheel` clients over HTTPS. The cached token self-heals:
it is refreshed proactively when its stored expiry has passed, and
reactively when the server rejects it (e.g. after the salt-master
container restarts and wipes its session store) — on rejection the CLI
discards the token, logs in again, and retries the request once.

Commands come in two layers:

- **Low-level** (`local`, `runner`, `wheel`) map directly to the salt-api
  clients and print **raw JSON**.
- **High-level** (`cmd`, `state`, `keys`) wrap those clients and render
  **readable, colorized output** with `rich`.

## Installation

```
pip install salt-api-cli
```

## Configuration

Configuration is resolved in this order (later sources override earlier):

1. `~/.saltapiclirc` — INI file, `[salt-api-cli]` section
2. Environment variables — `SALT_API_URL`, `SALT_API_USER`, `SALT_API_PASS`, `SALT_API_INSECURE`
3. Command-line flags — `--url`, `--user`, `--password`, `--insecure`, `--relogin`, `--no-token-cache`

Example `~/.saltapiclirc`:

```ini
[salt-api-cli]
url = https://salt.example.com
user = salt_api
password = secret
insecure = false
```

`SALT_API_INSECURE=1` (or `insecure = true` in the config) skips TLS
certificate verification.

Token cache control: `--relogin` ignores any cached token and logs in
fresh (re-caching the new token); `--no-token-cache` neither reads nor
writes the cache for that run; `salt logout` discards the cached token.

## Usage

### Low-level commands (raw JSON)

These map one-to-one to the salt-api clients and print the response
verbatim as indented JSON.

```
# Local client — fan out to minions
salt local '*' test.ping
salt local 'bml*' cmd.run 'whoami'
salt local 'bml1' cmd.run 'Get-Date' shell=powershell

# Runner client (master-side: manage.status, jobs.list_jobs, ...)
salt runner manage.status
salt runner jobs.list_jobs

# Wheel client (master-side, low-level)
salt wheel key.list_all
```

### High-level commands (readable, colorized)

These wrap the low-level clients and render their output with `rich`.

```
# Run a shell command — a live per-minion checklist while it runs, then
# one block per minion (exit code, stdout, stderr) and an ok/failed summary.
# Fired async (local_async + cmd.run_all) and polled via the runner, like
# `state`, so a slow or wide command never holds one long connection open.
salt cmd 'bml*' hostname
salt cmd 'bml1' 'Get-Date' shell=powershell

# State runs — a colored table of states, one row each, with a summary.
# Driven by the local client + state.* functions.
salt state highstate 'bml1'           # apply the highstate
salt state test 'bml1'                # dry-run the highstate (forces test=True)
salt state apply 'bml1' veyon         # apply specific sls module(s)
salt state apply 'bml1' veyon.ldap test=True

# Key management — wraps the wheel client's key.* functions.
# `keys list` shows one colored panel per status (Accepted/Pending/Denied/Rejected).
salt keys list
salt keys accept <id-or-glob>
salt keys accept-all
salt keys reject <id-or-glob>
salt keys delete <id-or-glob>
```

Color and panels appear when writing to a terminal; output is plain when
piped to a file or pager.

Any `key=value` argument is parsed as a kwarg to the salt function;
anything else is positional.

You can also invoke the CLI as a module: `python -m salt_api_cli ...`.

## License

This project is licensed under the terms of the MIT license.
