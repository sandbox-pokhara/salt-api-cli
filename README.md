# salt-api-cli

Thin, stdlib-only Python CLI for [salt-api](https://docs.saltproject.io/en/latest/ref/netapi/all/salt.netapi.rest_cherrypy.html).

Logs in once with PAM credentials, caches the token in
`~/.cache/salt-api-cli/token.json`, then invokes salt-api's `local`,
`runner`, and `wheel` clients over HTTPS. The token auto-refreshes
when it expires.

## Installation

```
pip install salt-api-cli
```

## Configuration

Configuration is resolved in this order (later sources override earlier):

1. `~/.saltapiclirc` — INI file, `[salt-api-cli]` section
2. Environment variables — `SALT_API_URL`, `SALT_API_USER`, `SALT_API_PASS`, `SALT_API_INSECURE`
3. Command-line flags — `--url`, `--user`, `--password`, `--insecure`

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

## Usage

```
# Local client — fan out to minions
salt-api-cli local '*' test.ping
salt-api-cli local 'bml*' cmd.run 'whoami'
salt-api-cli local 'bml1' cmd.run 'Get-Date' shell=powershell

# Runner client (master-side: manage.status, jobs.list_jobs, ...)
salt-api-cli runner manage.status
salt-api-cli runner jobs.list_jobs

# Wheel client (master-side, low-level)
salt-api-cli wheel key.list_all

# Key management (high-level wrapper around the wheel client)
salt-api-cli keys list
salt-api-cli keys accept <id-or-glob>
salt-api-cli keys accept-all
salt-api-cli keys reject <id-or-glob>
salt-api-cli keys delete <id-or-glob>
```

Any `key=value` argument is parsed as a kwarg to the salt function;
anything else is positional.

You can also invoke the CLI as a module: `python -m salt_api_cli ...`.

## License

This project is licensed under the terms of the MIT license.
