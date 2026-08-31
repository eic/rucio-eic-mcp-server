# Rucio MCP Server for EIC/ePIC

MCP server providing [Rucio](https://rucio.cern.ch/) data management tools for the Electron Ion Collider (EIC) ePIC experiment. Based on the [Belle II rucio-mcp server](https://gitlab.desy.de/belle2/computing/distributed-computing/developments/belleai-lab/rucio-mcp) by Cedric Serfon and Wouter Verkerke.

## Tools

| Tool | Description |
|------|-------------|
| `list_scopes` | List available Rucio scopes (discover what's on the server) |
| `list_dids` | Search for datasets/containers within a scope (name pattern + metadata filters) |
| `summarize_datasets` | Per-dataset file counts and sizes plus totals for all datasets matching a pattern, in one call |
| `list_files` | List files within a dataset or container |
| `get_did_metadata` | Get DID details (type, size, file count, custom fields) |
| `get_account_limits` | Get storage quota limits for an account |
| `get_account_usage` | Get storage usage for an account at a specific RSE |
| `list_rses` | List all Rucio Storage Elements |
| `get_rse_usage` | Get storage usage statistics for an RSE |
| `list_rules` | List replication rules with optional filters or for a specific DID |
| `get_rule_locks` | Get replica lock details for a replication rule |
| `list_file_replicas` | Find where file replicas are located across RSEs |
| `extract_scope` | Parse an EIC DID string into scope and name |

## Pagination

Large Rucio listings are paginated by default so page 1 is safe for chat and
stdio MCP clients. The list tools return at most 50 items unless `limit` is
specified, with a hard maximum of 500.

Paginated tools include `list_dids`, `summarize_datasets`, `list_files`, `list_content`,
`list_rules`, `get_rule_locks`, and `list_file_replicas`. Responses include a
`pagination` object:

```json
{
  "page": 1,
  "limit": 50,
  "total_count": 1621,
  "returned_count": 50,
  "has_more": true,
  "next_page": 2
}
```

Request the next page by passing `page=2` with the same query. For example:

```text
list_dids(scope="epic", name="*26.04.1*", type="DATASET", page=1, limit=25)
list_dids(scope="epic", name="*26.04.1*", type="DATASET", page=2, limit=25)
```

## ePIC campaign scopes

JLab ePIC production campaign datasets are commonly registered in the
`epic` scope with path-like DID names, for example:

```text
epic:/RECO/26.04.1/epic_craterlake/SINGLE/mu-/10GeV/etaScan
```

For campaign queries, search explicitly in the `epic` scope:

```text
list_dids(scope="epic", name="*26.04.1*", type="DATASET")
```

For managed data placement, query the replication rules for the DID:

```text
list_rules(did="epic:/RECO/26.04.1/epic_craterlake/SINGLE/gamma/100MeV/etaScan")
```

Direct XRootD paths under `/volatile/eic/EPIC/...` correspond to the same
Rucio DID names after stripping `/volatile/eic/EPIC`; for example
`/volatile/eic/EPIC/RECO/26.04.1` maps to `epic:/RECO/26.04.1`.

## Requirements

- Python 3.10+
- Network access to a Rucio instance (BNL or JLab)

## Authentication

Two methods supported, selected via `RUCIO_AUTH_TYPE`:

### X509 (default, for BNL Rucio)

```bash
export RUCIO_AUTH_TYPE=x509          # default
export X509_USER_PROXY=/tmp/x509     # path to proxy cert
export RUCIO_ACCOUNT=rucioddm
export RUCIO_URL=https://nprucio01.sdcc.bnl.gov:443
# Account 'panda' has X509 identities for IDDS/PanDA service certs
```

### Username/password (for JLab Rucio)

```bash
export RUCIO_AUTH_TYPE=userpass
export RUCIO_USERNAME=myuser
export RUCIO_PASSWORD=mypass
export RUCIO_ACCOUNT=myaccount
export RUCIO_URL=https://rucio.jlab.org:443
```

## Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `RUCIO_URL` | `https://nprucio01.sdcc.bnl.gov:443` | Rucio server URL |
| `RUCIO_AUTH_TYPE` | `x509` | Auth method: `x509` or `userpass` |
| `RUCIO_ACCOUNT` | `rucioddm` | Rucio account name |
| `X509_USER_PROXY` | `/tmp/x509` | Path to X509 proxy cert (x509 auth) |
| `RUCIO_USERNAME` | | Rucio username (userpass auth) |
| `RUCIO_PASSWORD` | | Rucio password (userpass auth) |
| `TOKEN_FILE_PATH` | `/tmp/rucio_eic_token.txt` | Cached auth token path |
| `RUCIO_CA_BUNDLE` | system default | CA bundle for TLS; `false` to disable |

## Usage

### stdio (for pandabot / Claude Code)

```bash
python rucio_eic_mcp_server.py
# or with argparse:
python rucio_eic_mcp_server.py --transport stdio
```

### Streamable HTTP (standalone server on port 9103)

```bash
python rucio_eic_mcp_server.py --transport http
```

The MCP endpoint is `http://127.0.0.1:9103/mcp` (change with `--host`,
`--port`, `--path`).  The server binds loopback by default; binding any
other address prints a warning because the tools carry no authentication.
The legacy SSE transport is still available with `--transport sse`.

### As installed package

```bash
pip install .
rucio-eic-mcp                                    # stdio
rucio-eic-mcp --transport http --port 9103       # streamable HTTP on /mcp
rucio-eic-mcp --transport sse                    # legacy SSE
```

## Setup for sys admins

For BNL (`nprucio01.sdcc.bnl.gov`):
1. X509 service certificate or auto-renewed proxy
2. Rucio account with read access to EIC scopes (`group.EIC`, `group.daq`, `user.*`)
3. Network access to `nprucio01.sdcc.bnl.gov:443`

For JLab (`rucio.jlab.org`):
1. Rucio username/password credentials
2. Network access to `rucio.jlab.org:443`
