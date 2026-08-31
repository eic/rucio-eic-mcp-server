"""
Rucio MCP Server for EIC/ePIC

MCP server providing Rucio data management tools for the Electron Ion Collider
(EIC) ePIC experiment. Exposes DIDs, replicas, RSEs, replication rules, and
account usage via the Model Context Protocol.

Authentication: X509 proxy certificate or username/password against BNL or
JLab Rucio instances.

Based on the Belle II rucio-mcp server by Cedric Serfon and Wouter Verkerke.
"""

import argparse
import os
import re
import sys
import time
from datetime import datetime
from json import loads
from typing import Any, Generator, Optional, Union

import requests
from urllib.parse import quote

from mcp.server.fastmcp import FastMCP

DEFAULT_PAGE_LIMIT = 50
MAX_PAGE_LIMIT = 500

EPIC_CAMPAIGN_PREFIXES = (
    "/RECO/",
    "/FULL/",
    "/SIM/",
    "/EVGEN/",
)
EPIC_STORAGE_PREFIXES = (
    "/volatile/eic/EPIC",
    "/volatile/eic/epic",
)


# ---------------------------------------------------------------------------
# Configuration from environment
# ---------------------------------------------------------------------------

CERT_PATH = os.getenv("X509_USER_PROXY", "/tmp/x509")
RUCIO_ACCOUNT = os.getenv("RUCIO_ACCOUNT", "rucioddm")
RUCIO_USERNAME = os.getenv("RUCIO_USERNAME", "")
RUCIO_PASSWORD = os.getenv("RUCIO_PASSWORD", "")
RUCIO_AUTH_TYPE = os.getenv("RUCIO_AUTH_TYPE", "x509")  # "x509" or "userpass"
RUCIO_VO = os.getenv("RUCIO_VO", "")  # required on multi-VO servers (e.g. BNL: "eic")
TOKEN_FILE_PATH = os.getenv("TOKEN_FILE_PATH", "/tmp/rucio_eic_token.txt")
RUCIO_URL = os.getenv("RUCIO_URL", "https://nprucio01.sdcc.bnl.gov:443")
# Use system CA bundle by default; override with RUCIO_CA_BUNDLE if needed.
# Set to "false" to disable verification (not recommended).
_ca_env = os.getenv("RUCIO_CA_BUNDLE", os.getenv("REQUESTS_CA_BUNDLE", ""))
if _ca_env.lower() == "false":
    CA_BUNDLE = False
elif _ca_env:
    CA_BUNDLE = _ca_env
else:
    CA_BUNDLE = True

AUTH_X509_URL = f"{RUCIO_URL}/auth/x509"
AUTH_USERPASS_URL = f"{RUCIO_URL}/auth/userpass"
DIDS_URL = f"{RUCIO_URL}/dids"
ACCOUNT_URL = f"{RUCIO_URL}/accounts"
RULES_URL = f"{RUCIO_URL}/rules"
RSES_URL = f"{RUCIO_URL}/rses"
REPLICAS_URL = f"{RUCIO_URL}/replicas"


# ---------------------------------------------------------------------------
# Response parsing helpers
# ---------------------------------------------------------------------------

def _load_json_data(response: requests.Response) -> Generator[Any, Any, Any]:
    """Parse streaming JSON responses (application/x-json-stream)."""
    if (
        "content-type" in response.headers
        and response.headers["content-type"] == "application/x-json-stream"
    ):
        for line in response.iter_lines():
            if line:
                yield _parse_response(line)
    else:
        if response.text:
            yield response.text


def _datetime_parser(dct: dict[Any, Any]) -> dict[Any, Any]:
    """JSON object_hook that converts '... UTC' strings to datetime objects."""
    for k, v in list(dct.items()):
        if isinstance(v, str) and re.search(" UTC", v):
            try:
                dct[k] = datetime.strptime(v, "%Y-%m-%d %H:%M:%S UTC")
            except Exception:
                pass
    return dct


def _parse_response(data: Union[str, bytes, bytearray]) -> Any:
    """Decode and parse a JSON response line."""
    if isinstance(data, (bytes, bytearray)):
        data = data.decode("utf-8")
    return loads(data, object_hook=_datetime_parser)


# ---------------------------------------------------------------------------
# Rucio HTTP helpers
# ---------------------------------------------------------------------------

def _make_rucio_request(
    url: str,
    method: str = "GET",
    headers: dict = None,
    payload: dict = None,
    params: dict = None,
) -> dict:
    """
    Make a request to the Rucio REST API.

    Returns dict with 'status' and 'data' on success, or 'error' on failure.
    """
    if headers is None:
        headers = {}
    try:
        response = requests.request(
            method, url, headers=headers, json=payload, params=params,
            verify=CA_BUNDLE, timeout=60,
        )
        response.raise_for_status()
        if response.headers.get("Content-Type") == "application/x-json-stream":
            return {"status": response.status_code, "data": list(_load_json_data(response))}
        if response.text:
            return {"status": response.status_code, "data": _parse_response(response.text)}
        return {"status": response.status_code, "data": None}
    except requests.exceptions.RequestException as e:
        return {"error": str(e)}


def _pagination_window(page: int = 1, limit: int = DEFAULT_PAGE_LIMIT) -> tuple[int, int, int, int]:
    """Return normalized (page, limit, start, end) for in-memory result paging."""
    try:
        page = int(page)
    except (TypeError, ValueError):
        page = 1
    try:
        limit = int(limit)
    except (TypeError, ValueError):
        limit = DEFAULT_PAGE_LIMIT
    page = max(1, page)
    limit = max(1, min(limit, MAX_PAGE_LIMIT))
    start = (page - 1) * limit
    return page, limit, start, start + limit


def _paginate_items(items: list, page: int = 1, limit: int = DEFAULT_PAGE_LIMIT) -> tuple[list, dict]:
    """Slice a list and return page metadata."""
    page, limit, start, end = _pagination_window(page, limit)
    total = len(items)
    return items[start:end], {
        "page": page,
        "limit": limit,
        "total_count": total,
        "returned_count": max(0, min(end, total) - min(start, total)),
        "has_more": end < total,
        "next_page": page + 1 if end < total else None,
    }


def _paginate_data_result(
    result: dict,
    page: int = 1,
    limit: int = DEFAULT_PAGE_LIMIT,
    nested_list_key: str | None = None,
) -> dict:
    """
    Apply response-size-safe pagination to Rucio list results.

    Rucio bulk file listing returns one DID wrapper containing a large `files`
    list; other list endpoints generally return the large list directly as
    result["data"]. Both shapes are handled here.
    """
    if "error" in result:
        return result
    data = result.get("data")
    paged = dict(result)

    if (
        nested_list_key
        and isinstance(data, list)
        and len(data) == 1
        and isinstance(data[0], dict)
        and isinstance(data[0].get(nested_list_key), list)
    ):
        item = dict(data[0])
        sliced, pagination = _paginate_items(item[nested_list_key], page, limit)
        item[nested_list_key] = sliced
        pagination["item_path"] = f"data[0].{nested_list_key}"
        paged["data"] = [item]
        paged["pagination"] = pagination
        return paged

    if isinstance(data, list):
        sliced, pagination = _paginate_items(data, page, limit)
        pagination["item_path"] = "data"
        paged["data"] = sliced
        paged["pagination"] = pagination

    return paged


# ---------------------------------------------------------------------------
# Authentication — X509 or userpass
# ---------------------------------------------------------------------------

def _get_token_x509() -> dict:
    """Authenticate with Rucio via X509 proxy certificate."""
    headers = {"X-Rucio-Account": RUCIO_ACCOUNT}
    if RUCIO_VO:
        headers["X-Rucio-VO"] = RUCIO_VO
    try:
        cert = (CERT_PATH, CERT_PATH)
        response = requests.get(
            AUTH_X509_URL, headers=headers, verify=CA_BUNDLE, stream=True,
            cert=cert, timeout=15,
        )
        response.raise_for_status()
        token = response.headers.get("X-Rucio-Auth-Token")
        if not token:
            return {"error": "Token not found in Rucio response headers."}
        with open(TOKEN_FILE_PATH, "w") as f:
            f.write(token)
        return {"status": response.status_code, "message": "Token stored successfully."}
    except requests.exceptions.RequestException as e:
        return {"error": f"Rucio X509 authentication failed: {e}"}


def _get_token_userpass() -> dict:
    """Authenticate with Rucio via username/password."""
    headers = {
        "X-Rucio-Account": RUCIO_ACCOUNT,
        "X-Rucio-Username": RUCIO_USERNAME,
        "X-Rucio-Password": RUCIO_PASSWORD,
    }
    if RUCIO_VO:
        headers["X-Rucio-VO"] = RUCIO_VO
    try:
        response = requests.get(
            AUTH_USERPASS_URL, headers=headers, verify=CA_BUNDLE, timeout=15,
        )
        response.raise_for_status()
        token = response.headers.get("X-Rucio-Auth-Token")
        if not token:
            return {"error": "Token not found in Rucio response headers."}
        with open(TOKEN_FILE_PATH, "w") as f:
            f.write(token)
        return {"status": response.status_code, "message": "Token stored successfully."}
    except requests.exceptions.RequestException as e:
        return {"error": f"Rucio userpass authentication failed: {e}"}


def _get_token() -> dict:
    """Obtain a Rucio auth token using the configured auth method."""
    if RUCIO_AUTH_TYPE == "userpass":
        return _get_token_userpass()
    return _get_token_x509()


def _get_token_from_file() -> str:
    """
    Retrieve the cached Rucio auth token, refreshing if expired (>1 hour).

    Raises RuntimeError if authentication fails.
    """
    refresh = False
    if not os.path.exists(TOKEN_FILE_PATH):
        refresh = True
    else:
        stat = os.stat(TOKEN_FILE_PATH)
        if time.time() - stat.st_mtime > 3600:
            refresh = True

    if refresh:
        result = _get_token()
        if "error" in result:
            raise RuntimeError(result["error"])

    with open(TOKEN_FILE_PATH, "r") as f:
        token = f.read().strip()
    if not token:
        raise RuntimeError("Token file is empty. Rucio authentication may have failed.")
    return token


def _rucio_headers(accept: str = "application/json") -> dict:
    """Build standard Rucio API headers with a valid auth token."""
    token = _get_token_from_file()
    return {
        "X-Rucio-Auth-Token": token,
        "Content-Type": "application/json",
        "Accept": accept,
    }


# ---------------------------------------------------------------------------
# EIC/ePIC scope extraction
# ---------------------------------------------------------------------------

def _extract_scope_eic(did: str) -> dict[str, str]:
    """
    Extract scope and name from an EIC/ePIC DID string.

    EIC naming conventions:
    - epic:/RECO/...             — JLab ePIC campaign datasets
    - /RECO/...                  — JLab ePIC campaign dataset names in epic scope
    - /volatile/eic/EPIC/RECO/... — XRootD path corresponding to epic:/RECO/...
    - group.EIC:dataset_name       — ePIC production datasets
    - group.daq:swf.NNNNNN.run    — streaming workflow / DAQ datasets
    - user.<username>:name         — user datasets
    - group.<group>:name           — group datasets

    If the DID contains a colon, it's already scope:name.
    Otherwise, infer from path conventions.
    """
    # Already has explicit scope
    if ":" in did:
        scope, name = did.split(":", 1)
        return {"scope": scope, "name": name}

    # JLab ePIC campaign DIDs use the flat "epic" scope with path-like
    # names such as /RECO/26.04.1/epic_craterlake/....
    if did.startswith(EPIC_CAMPAIGN_PREFIXES):
        return {"scope": "epic", "name": did}

    # Convert common XRootD paths to the matching Rucio DID name.
    for prefix in EPIC_STORAGE_PREFIXES:
        if did.startswith(prefix + "/"):
            name = did[len(prefix):]
            if name.startswith(EPIC_CAMPAIGN_PREFIXES):
                return {"scope": "epic", "name": name}

    # Path-based inference for EIC conventions
    if did.startswith("/eic/") or did.startswith("/EIC/"):
        parts = did.split("/")
        # /eic/user/<username>/... → user.<username>
        if len(parts) > 3 and parts[2] == "user":
            return {"scope": f"user.{parts[3]}", "name": did}
        # /eic/group/<group>/... → group.<group>
        if len(parts) > 3 and parts[2] == "group":
            return {"scope": f"group.{parts[3]}", "name": did}
        return {"scope": "group.EIC", "name": did}

    # Streaming workflow datasets
    if did.startswith("swf."):
        return {"scope": "group.daq", "name": did}

    # Default to group.EIC for unrecognized patterns
    return {"scope": "group.EIC", "name": did}


# ---------------------------------------------------------------------------
# MCP Server
# ---------------------------------------------------------------------------

mcp = FastMCP(
    "rucio-eic",
    stateless_http=True,
    json_response=False,
    host="127.0.0.1",
    port=9103,
)


@mcp.tool(description="List available Rucio scopes.")
def list_scopes() -> dict:
    """
    Fetch all available scopes from the Rucio instance.

    Returns the list of scopes (e.g., group.EIC, group.daq, user.wenaus, ...).
    Use this first to discover what data is available on the server.
    """
    try:
        headers = _rucio_headers()
    except RuntimeError as e:
        return {"error": str(e)}

    return _make_rucio_request(f"{RUCIO_URL}/scopes", headers=headers)


@mcp.tool(description="Search for DIDs (datasets/containers) within a scope, filtered by name pattern and/or metadata key=value filters.")
def list_dids(
    scope: str,
    name: Optional[str] = None,
    type: str = "DATASET",
    filters: Optional[dict[str, str]] = None,
    long: bool = False,
    page: int = 1,
    limit: int = DEFAULT_PAGE_LIMIT,
) -> dict:
    """
    Search for DIDs within a scope — live Rucio query; seconds for narrow patterns, longer for broad wildcards over a large scope.

    Optional name-pattern and metadata-filter criteria.

    Equivalent to the Rucio CLI:
        rucio did list "scope:pattern" --filter "key=value,key2=value2"

    Args:
        scope: Rucio scope (e.g., 'group.EIC', 'group.daq', 'user.wenaus', 'epic').
        name: Optional name pattern filter. Supports Rucio wildcards '*' and '?'.
              Example: '*26.03.1*' matches any DID name containing '26.03.1'.
        type: DID type filter — DATASET (default), CONTAINER, FILE, or ALL.
              Note: COLLECTION (datasets+containers) is not supported by all Rucio
              versions; DATASET is the reliable default. Use ALL to span everything
              (can be huge — combine with name or filters).
        filters: Optional dict of metadata key=value filters. Values may use
              Rucio wildcards ('*' and '?'). Examples of populated ePIC keys
              (campaign 26.03.x and later): requester_pwg (inclusive, exclusive,
              semi-inclusive, jets, ...), generator (beagle, pythia8, ...),
              software_release ('26.03.*'), data_level (reconstruction,
              simulation), electron_beam_energy_gev, ion_beam_energy_gev,
              ion_species, geometry_config, q2_min_gev2, q2_max_gev2,
              is_background_mixed. Run get_did_metadata on a sample 26.03.x DID
              (plugin='ALL') to see the full schema. The server silently returns
              0 matches for unpopulated keys — pre-26.03 DIDs have none.
        long: If True, return full DID info (type, bytes, length, ...) instead of
              just name. Useful when pairing a metadata search with inspection.
        page: Result page number to return. Defaults to page 1.
        limit: Number of DIDs per page. Defaults to 50, maximum 500.
    """
    try:
        headers = _rucio_headers("application/x-json-stream")
    except RuntimeError as e:
        return {"error": str(e)}

    url = f"{DIDS_URL}/{scope}/dids/search"
    params: dict[str, str] = {"type": type}
    if name:
        params["name"] = name
    if long:
        params["long"] = "True"
    if filters:
        for k, v in filters.items():
            if k in params:
                return {"error": f"filter key '{k}' conflicts with a reserved parameter"}
            params[k] = v
    result = _make_rucio_request(url, headers=headers, params=params)

    # Auto-retry: if the caller asked for CONTAINER with a name or filter
    # and the server returned an empty list, retry as DATASET. Many Rucio
    # scopes (notably JLab's 'epic') hold flat datasets, not containers,
    # so a CONTAINER search silently returns nothing.
    if (
        type == "CONTAINER"
        and (name or filters)
        and "error" not in result
        and result.get("data") in (None, [], "")
    ):
        params["type"] = "DATASET"
        retry = _make_rucio_request(url, headers=headers, params=params)
        if "error" not in retry and retry.get("data"):
            retry["hint"] = (
                "type=CONTAINER returned 0 results; showing DATASET results "
                "instead. This scope appears to hold datasets, not containers."
            )
            return _paginate_data_result(retry, page=page, limit=limit)

    return _paginate_data_result(result, page=page, limit=limit)


@mcp.tool(description="List files within a Rucio dataset or container.")
def list_files(
    scope: str,
    name: str,
    page: int = 1,
    limit: int = DEFAULT_PAGE_LIMIT,
) -> dict:
    """
    Fetch the file listing for one Rucio DID — live Rucio query, seconds per dataset.

    Args:
        scope: Rucio scope (e.g., 'group.EIC').
        name: DID name (e.g., 'epic.26.02.0.ePIC_craterlake.p1001.e1.s1.r1').
        page: Result page number to return. Defaults to page 1.
        limit: Number of files per page. Defaults to 50, maximum 500.
    """
    try:
        headers = _rucio_headers("application/x-json-stream")
    except RuntimeError as e:
        return {"error": str(e)}

    url = f"{DIDS_URL}/bulkfiles"
    payload = {"dids": [{"scope": scope, "name": name}]}
    result = _make_rucio_request(url, method="POST", headers=headers, payload=payload)
    return _paginate_data_result(result, page=page, limit=limit, nested_list_key="files")


@mcp.tool(description="List immediate children of a Rucio container or dataset.")
def list_content(
    scope: str,
    name: str,
    page: int = 1,
    limit: int = DEFAULT_PAGE_LIMIT,
) -> dict:
    """
    List the child DIDs within a container or dataset (one level).

    For containers: returns child datasets and/or containers.
    For datasets: returns child files.
    Use list_files for a recursive listing of all files.

    Args:
        scope: Rucio scope (e.g., 'epic', 'group.EIC').
        name: DID name (may contain slashes, e.g., '/RECO/26.03.1/...').
        page: Result page number to return. Defaults to page 1.
        limit: Number of child DIDs per page. Defaults to 50, maximum 500.
    """
    try:
        headers = _rucio_headers("application/x-json-stream")
    except RuntimeError as e:
        return {"error": str(e)}

    url = f"{DIDS_URL}/{quote(scope, safe='')}/{quote(name, safe='')}/dids"
    result = _make_rucio_request(url, headers=headers)
    return _paginate_data_result(result, page=page, limit=limit)


@mcp.tool(description="Get DID details — system fields plus any custom physics metadata (pwg, generator, software_release, beam energies, Q2, ion species, ...).")
def get_did_metadata(scope: str, name: str, plugin: str = "ALL") -> dict:
    """
    Fetch full details for a DID (Data Identifier), merging system columns
    with custom physics metadata.

    Args:
        scope: Rucio scope.
        name: DID name.
        plugin: Metadata plugin selector. Default "ALL" returns both Rucio
            system columns and custom JSON metadata (requester_pwg, generator,
            software_release, electron_beam_energy_gev, ion_beam_energy_gev,
            ion_species, geometry_config, q2_min_gev2, q2_max_gev2,
            data_level, is_background_mixed, ...). Other options:
            "DID_COLUMN" (system columns only), "POSTGRES_JSON" (custom JSON
            only). Availability of custom metadata depends on server config
            and campaign — ePIC populates it from 26.03.x onward.
    """
    try:
        headers = _rucio_headers()
    except RuntimeError as e:
        return {"error": str(e)}

    url = f"{DIDS_URL}/{quote(scope, safe='')}/{quote(name, safe='')}/meta"
    # JLab's Rucio rejects the plugin selector (plugin=ALL returns 404 and
    # the JSON plugin is not enabled on the server), while the
    # parameterless call returns everything the server supports. Send the
    # selector only when the caller names a specific non-default plugin.
    params = {"plugin": plugin} if plugin and plugin != "ALL" else None
    return _make_rucio_request(url, headers=headers, params=params)


# Widest match set summarize_datasets will reduce in one call; a broader
# pattern gets an error advising a narrower one rather than a huge fetch.
SUMMARY_MAX_DATASETS = 2000


def _iso_utc(value) -> Optional[str]:
    """Rucio timestamps ('Thu, 04 Jun 2026 19:10:37 UTC' strings, or the
    datetimes _datetime_parser makes of them) as sortable ISO-8601 UTC."""
    if isinstance(value, datetime):
        return value.strftime("%Y-%m-%dT%H:%M:%SZ")
    if isinstance(value, str):
        try:
            return datetime.strptime(
                value, "%a, %d %b %Y %H:%M:%S %Z"
            ).strftime("%Y-%m-%dT%H:%M:%SZ")
        except ValueError:
            return value
    return None


@mcp.tool(description="Summarize all datasets matching a name pattern in ONE call: per-dataset file counts and sizes plus totals. Use this for any 'how many files / how much data' question — never loop get_did_metadata or list_files over datasets.")
def summarize_datasets(
    scope: str,
    name: Optional[str] = None,
    filters: Optional[dict[str, str]] = None,
    order: str = "name",
    page: int = 1,
    limit: int = DEFAULT_PAGE_LIMIT,
) -> dict:
    """
    One-call summary of every dataset matching a pattern — live Rucio walk of the full match set; broad wildcards can take a minute or more and may time out.

    Per-dataset file count, byte size, and created/updated times, plus
    totals over the full match set.

    Example — "summarize the file counts for the datasets under
    epic:/EVGEN":
        summarize_datasets(scope='epic', name='/EVGEN/*')
    Example — "what are the latest added/updated EVGEN datasets":
        summarize_datasets(scope='epic', name='/EVGEN/*', order='updated')

    Args:
        scope: Rucio scope (e.g., 'epic', 'group.EIC').
        name: Optional name pattern with Rucio wildcards '*' and '?'
              (e.g., '/EVGEN/*', '*26.03.1*').
        filters: Optional dict of metadata key=value filters, as in
              list_dids.
        order: Row order — 'name' (default, ascending), or 'updated' /
              'created' (newest first; page 1 is the latest datasets).
        page: Dataset-row page to return. The totals block always covers
            every match regardless of paging.
        limit: Dataset rows per page. Defaults to 50, maximum 500.

    Returns:
        totals: {datasets, files, bytes, unknown} over ALL matches —
            'unknown' counts datasets whose size Rucio does not report.
        data: per-dataset rows [{name, files, bytes, created, updated}],
            ordered and paginated.
    """
    try:
        headers = _rucio_headers("application/x-json-stream")
    except RuntimeError as e:
        return {"error": str(e)}

    url = f"{DIDS_URL}/{scope}/dids/search"
    params: dict[str, str] = {"type": "DATASET", "long": "True"}
    if name:
        params["name"] = name
    if filters:
        for k, v in filters.items():
            if k in params:
                return {"error": f"filter key '{k}' conflicts with a reserved parameter"}
            params[k] = v
    result = _make_rucio_request(url, headers=headers, params=params)
    if "error" in result:
        return result

    entries = result.get("data") or []
    if len(entries) > SUMMARY_MAX_DATASETS:
        return {"error": (
            f"{len(entries)} datasets match — more than the "
            f"{SUMMARY_MAX_DATASETS} this summary will reduce. "
            "Narrow the name pattern or filters."
        )}

    rows = []
    for entry in entries:
        if not isinstance(entry, dict):
            continue
        row = {
            "name": entry.get("name"),
            "files": entry.get("length"),
            "bytes": entry.get("bytes"),
            "created": _iso_utc(entry.get("created_at")),
            "updated": _iso_utc(entry.get("updated_at")),
        }
        # The search's long form leaves length/bytes/dates unset on some
        # servers (JLab); the dataset replicas carry them. Counts are the
        # max across RSEs (Rucio reports identical totals per replica);
        # created is the earliest replica, updated the latest touch.
        if (row["files"] is None or row["updated"] is None) and row["name"]:
            rep = _make_rucio_request(
                f"{REPLICAS_URL}/{quote(scope, safe='')}/"
                f"{quote(row['name'], safe='')}/datasets",
                headers=headers,
            )
            replicas = [r for r in (rep.get("data") or [])
                        if isinstance(r, dict)]
            counted = [r for r in replicas if r.get("length") is not None]
            if counted and row["files"] is None:
                row["files"] = max(r["length"] for r in counted)
                row["bytes"] = max(r.get("bytes") or 0 for r in counted)
            created = [c for c in (_iso_utc(r.get("created_at"))
                                   for r in replicas) if c]
            updated = [u for u in (_iso_utc(r.get("updated_at"))
                                   for r in replicas) if u]
            if row["created"] is None and created:
                row["created"] = min(created)
            if row["updated"] is None and updated:
                row["updated"] = max(updated)
        rows.append(row)
    if order in ("updated", "created"):
        # ISO strings sort chronologically; undated rows land last.
        rows.sort(key=lambda r: r[order] or "", reverse=True)
    else:
        rows.sort(key=lambda r: r["name"] or "")

    totals = {
        "datasets": len(rows),
        "files": sum(r["files"] for r in rows if r["files"] is not None),
        "bytes": sum(r["bytes"] for r in rows if r["bytes"] is not None),
        "unknown": sum(1 for r in rows if r["files"] is None),
    }
    paged = _paginate_data_result(
        {"status": result.get("status"), "data": rows},
        page=page, limit=limit,
    )
    paged["totals"] = totals
    return paged


@mcp.tool(description="Get storage quota limits for a Rucio account.")
def get_account_limits(account: str) -> dict:
    """
    Fetch account storage limits across all RSEs.

    Args:
        account: Rucio account name (e.g., 'wenaus', 'rucioddm').
    """
    try:
        headers = _rucio_headers()
    except RuntimeError as e:
        return {"error": str(e)}

    url = f"{ACCOUNT_URL}/{account}/limits"
    return _make_rucio_request(url, headers=headers)


@mcp.tool(description="Get storage usage for a Rucio account at a specific RSE.")
def get_account_usage(account: str, rse: str) -> dict:
    """
    Fetch storage usage for an account at a specific RSE.

    Args:
        account: Rucio account name.
        rse: RSE name (e.g., 'BNL_SDCC_EIC', 'JLAB_EIC').
    """
    try:
        headers = _rucio_headers()
    except RuntimeError as e:
        return {"error": str(e)}

    url = f"{ACCOUNT_URL}/{account}/usage/local/{rse}"
    return _make_rucio_request(url, headers=headers)


@mcp.tool(description="List all Rucio Storage Elements (RSEs).")
def list_rses() -> dict:
    """
    Fetch the list of all RSEs (Rucio Storage Elements).

    Returns RSE names, availability, and basic configuration.
    EIC RSEs include BNL_SDCC_EIC, JLAB_EIC, etc.
    """
    try:
        headers = _rucio_headers("application/x-json-stream")
    except RuntimeError as e:
        return {"error": str(e)}

    return _make_rucio_request(RSES_URL, headers=headers)


@mcp.tool(description="Get storage usage statistics for a specific RSE.")
def get_rse_usage(rse: str) -> dict:
    """
    Fetch usage statistics for an RSE (used, free, total bytes).

    Args:
        rse: RSE name (e.g., 'BNL_SDCC_EIC').
    """
    try:
        headers = _rucio_headers("application/x-json-stream")
    except RuntimeError as e:
        return {"error": str(e)}

    url = f"{RSES_URL}/{rse}/usage"
    return _make_rucio_request(url, headers=headers)


@mcp.tool(description="List replication rules with optional filters, including a specific DID by scope/name.")
def list_rules(
    scope: Optional[str] = None,
    name: Optional[str] = None,
    did: Optional[str] = None,
    filters: Optional[dict[str, str]] = None,
    page: int = 1,
    limit: int = DEFAULT_PAGE_LIMIT,
) -> dict:
    """
    Fetch replication rules, optionally filtered.

    Args:
        scope: Optional Rucio scope for a specific DID, e.g. 'epic'.
        name: Optional Rucio DID name for a specific DID, e.g.
            '/RECO/26.04.1/epic_craterlake/SINGLE/gamma/100MeV/etaScan'.
        did: Optional combined DID string. If provided without scope/name,
            extract_scope() is used, so 'epic:/RECO/...' and
            '/volatile/eic/EPIC/RECO/...' both work.
        filters: Optional dict of filters:
            - account: Filter by Rucio account.
            - state: Filter by rule state — 'O' (OK), 'R' (Replicating), 'S' (Stuck).
            - rse_expression: Filter by destination RSE expression.
            Example: {"account": "wenaus", "state": "R"}
        page: Result page number to return. Defaults to page 1.
        limit: Number of rules per page. Defaults to 50, maximum 500.
    """
    try:
        headers = _rucio_headers("application/x-json-stream")
    except RuntimeError as e:
        return {"error": str(e)}

    params = dict(filters) if filters else {}
    if did and not (scope and name):
        parsed = _extract_scope_eic(did)
        scope = parsed["scope"]
        name = parsed["name"]
    if scope:
        params["scope"] = scope
    if name:
        params["name"] = name
    result = _make_rucio_request(RULES_URL, headers=headers, params=params)
    return _paginate_data_result(result, page=page, limit=limit)


@mcp.tool(description="Get replica lock details for a replication rule.")
def get_rule_locks(
    rule_id: str,
    page: int = 1,
    limit: int = DEFAULT_PAGE_LIMIT,
) -> dict:
    """
    Fetch replica locks associated with a replication rule.

    Args:
        rule_id: The Rucio rule ID (UUID).
        page: Result page number to return. Defaults to page 1.
        limit: Number of locks per page. Defaults to 50, maximum 500.
    """
    try:
        headers = _rucio_headers("application/x-json-stream")
    except RuntimeError as e:
        return {"error": str(e)}

    url = f"{RULES_URL}/{rule_id}/locks"
    result = _make_rucio_request(url, headers=headers)
    return _paginate_data_result(result, page=page, limit=limit)


@mcp.tool(description="Find where file replicas are located across RSEs.")
def list_file_replicas(
    dids: list[dict[str, str]],
    page: int = 1,
    limit: int = DEFAULT_PAGE_LIMIT,
) -> dict:
    """
    Fetch replica locations for a list of DIDs — live Rucio query, roughly a second per DID; keep the list short.

    Args:
        dids: List of DIDs, each a dict with 'scope' and 'name'.
              Example: [{"scope": "group.EIC", "name": "file.root"}]
        page: Result page number to return. Defaults to page 1.
        limit: Number of replica entries per page. Defaults to 50, maximum 500.

    Returns replica locations (RSEs and PFNs) for each file.
    """
    try:
        headers = _rucio_headers("application/x-json-stream")
    except RuntimeError as e:
        return {"error": str(e)}

    url = f"{REPLICAS_URL}/list"
    payload = {"dids": dids}
    result = _make_rucio_request(url, method="POST", headers=headers, payload=payload)
    return _paginate_data_result(result, page=page, limit=limit)


@mcp.tool(description="Extract scope and name from an EIC DID string.")
def extract_scope(did: str) -> dict[str, str]:
    """
    Parse an EIC/ePIC DID string into scope and name components.

    Handles:
    - Explicit scope:name format (e.g., 'group.EIC:dataset_name')
    - EIC path conventions (/eic/user/..., /eic/group/...)
    - SWF dataset names (swf.NNNNNN.run)

    Args:
        did: The DID string to parse.
    """
    return _extract_scope_eic(did)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main():
    """Run the MCP server (stdio by default, streamable HTTP on request)."""
    parser = argparse.ArgumentParser(description="Rucio EIC MCP Server")
    parser.add_argument(
        "--transport",
        choices=["stdio", "http", "streamable-http", "sse"],
        default="stdio",
        help="Transport to serve on (default: stdio). 'http' is streamable HTTP; "
        "'sse' is the legacy SSE transport.",
    )
    parser.add_argument(
        "--host",
        default="127.0.0.1",
        help="Bind address for the HTTP transports (default: 127.0.0.1).",
    )
    parser.add_argument(
        "--port",
        type=int,
        default=9103,
        help="TCP port for the HTTP transports (default: 9103).",
    )
    parser.add_argument(
        "--path",
        default="/mcp",
        help="URL path the streamable-HTTP endpoint is served on (default: /mcp).",
    )
    args = parser.parse_args()

    if args.transport == "stdio":
        mcp.run(transport="stdio")
        return

    # Set post-construction so FASTMCP_* env vars / .env cannot override the CLI.
    mcp.settings.host = args.host
    mcp.settings.port = args.port
    mcp.settings.streamable_http_path = args.path

    if args.host not in {"127.0.0.1", "::1", "localhost"}:
        print(
            f"WARNING: --host {args.host} exposes every tool on a non-loopback "
            "interface with no authentication.",
            file=sys.stderr,
        )

    mcp.run(transport="sse" if args.transport == "sse" else "streamable-http")


if __name__ == "__main__":
    main()
