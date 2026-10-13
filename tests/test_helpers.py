from datetime import datetime
import requests

import rucio_eic_mcp_server as server


def test_extract_scope_eic_handles_campaign_prefixes() -> None:
    assert server._extract_scope_eic("/RECO/26.03.1/epic_craterlake/test") == {
        "scope": "epic",
        "name": "/RECO/26.03.1/epic_craterlake/test",
    }
    assert server._extract_scope_eic("/volatile/eic/EPIC/RECO/26.03.1/epic_craterlake/test") == {
        "scope": "epic",
        "name": "/RECO/26.03.1/epic_craterlake/test",
    }


def test_extract_scope_eic_handles_group_and_user_paths() -> None:
    assert server._extract_scope_eic("/eic/user/wenaus/my_dataset") == {
        "scope": "user.wenaus",
        "name": "/eic/user/wenaus/my_dataset",
    }
    assert server._extract_scope_eic("/eic/group/EIC/my_dataset") == {
        "scope": "group.EIC",
        "name": "/eic/group/EIC/my_dataset",
    }
    assert server._extract_scope_eic("swf.12345.run") == {
        "scope": "group.daq",
        "name": "swf.12345.run",
    }


def test_pagination_window_clamps_invalid_values() -> None:
    assert server._pagination_window(page=2, limit=10) == (2, 10, 10, 20)
    assert server._pagination_window(page=-3, limit=0) == (1, 1, 0, 1)
    assert server._pagination_window(page=999, limit=9999) == (999, 500, 499000, 499500)


def test_paginate_data_result_slices_nested_lists() -> None:
    result = {"status": 200, "data": [{"files": ["a", "b", "c", "d"]}]}
    paged = server._paginate_data_result(result, page=2, limit=2, nested_list_key="files")

    assert paged["data"][0]["files"] == ["c", "d"]
    assert paged["pagination"]["page"] == 2
    assert paged["pagination"]["returned_count"] == 2
    assert paged["pagination"]["has_more"] is False


def test_parse_response_converts_utc_strings_to_datetimes() -> None:
    payload = '{"created_at": "2024-12-31 00:00:00 UTC"}'
    parsed = server._parse_response(payload)

    assert parsed["created_at"] == datetime(2024, 12, 31, 0, 0, 0)


def test_paginate_items_notes_truncation() -> None:
    _, pagination = server._paginate_items(list(range(120)), page=1, limit=50)
    assert pagination["note"] == (
        "truncated: 50 of 120 shown; pass page=2 (limit up to 500) or narrow the query"
    )
    _, pagination = server._paginate_items(list(range(10)), page=1, limit=50)
    assert "note" not in pagination


def test_make_rucio_request_timeout_returns_explanatory_error(monkeypatch) -> None:
    def raise_timeout(*args, **kwargs):
        raise requests.exceptions.Timeout("read timed out")

    monkeypatch.setattr(server.requests, "request", raise_timeout)
    result = server._make_rucio_request("https://rucio.invalid/dids", timeout=7, tool_name="list_dids")

    assert result["error"].startswith("list_dids: Rucio did not answer within 7 s.")
    assert "retry once later" in result["error"]


def test_make_rucio_request_reports_rucio_exception_class(monkeypatch) -> None:
    response = requests.Response()
    response.status_code = 404
    response.url = "https://rucio.invalid/dids/epic/x/meta"
    response.headers["ExceptionClass"] = "DataIdentifierNotFound"
    response.headers["ExceptionMessage"] = "Data identifier 'epic:x' not found"

    def raise_http_error(*args, **kwargs):
        raise requests.exceptions.HTTPError(response=response)

    monkeypatch.setattr(server.requests, "request", raise_http_error)
    result = server._make_rucio_request(response.url, tool_name="get_did_metadata")

    assert result["error"] == (
        "get_did_metadata: DataIdentifierNotFound: Data identifier 'epic:x' not found; "
        "check the scope and name with list_dids or extract_scope"
    )


def test_missing_x509_proxy_is_an_error_dict(monkeypatch, tmp_path) -> None:
    monkeypatch.setattr(server, "CERT_PATH", str(tmp_path / "missing_x509"))
    monkeypatch.setattr(server, "TOKEN_FILE_PATH", str(tmp_path / "token"))
    monkeypatch.setattr(server, "RUCIO_AUTH_TYPE", "x509")

    result = server.list_scopes()

    assert "X509 proxy not found" in result["error"]
    assert "X509_USER_PROXY" in result["error"]


def test_instructions_list_every_tool() -> None:
    for tool in ("list_scopes", "list_dids", "summarize_datasets", "list_files",
                 "list_content", "get_did_metadata", "get_account_limits",
                 "get_account_usage", "list_rses", "get_rse_usage", "list_rules",
                 "get_rule_locks", "list_file_replicas", "extract_scope"):
        assert tool in server.mcp.instructions
