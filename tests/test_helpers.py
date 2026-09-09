from datetime import datetime

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
