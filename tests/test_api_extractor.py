"""Tests for API extractor."""

from __future__ import annotations

import json
from io import BytesIO
from unittest.mock import MagicMock, patch
from typing import Any

from pipeflow.extractors.api import APIExtractor


def _mock_response(data: Any, headers: dict[str, str] | None = None) -> MagicMock:
    """Create a mock HTTP response."""
    body = json.dumps(data).encode("utf-8")
    resp = MagicMock()
    resp.read.return_value = body
    resp.__enter__ = lambda s: s
    resp.__exit__ = MagicMock(return_value=False)
    mock_headers = MagicMock()
    mock_headers.get = lambda key, default="": (headers or {}).get(key, default)
    resp.headers = mock_headers
    return resp


class TestAPIExtractor:
    @patch("pipeflow.extractors.api.urllib.request.urlopen")
    def test_simple_array(self, mock_urlopen: MagicMock) -> None:
        data = [{"id": 1, "name": "Alice"}, {"id": 2, "name": "Bob"}]
        mock_urlopen.return_value = _mock_response(data)

        ext = APIExtractor(url="https://api.example.com/users")
        records = list(ext.extract())
        assert len(records) == 2
        assert records[0]["name"] == "Alice"

    @patch("pipeflow.extractors.api.urllib.request.urlopen")
    def test_nested_results(self, mock_urlopen: MagicMock) -> None:
        data = {"results": [{"id": 1}, {"id": 2}], "total": 2}
        mock_urlopen.return_value = _mock_response(data)

        ext = APIExtractor(url="https://api.example.com/users")
        records = list(ext.extract())
        assert len(records) == 2

    @patch("pipeflow.extractors.api.urllib.request.urlopen")
    def test_pagination_via_next_url(self, mock_urlopen: MagicMock) -> None:
        page1 = {"results": [{"id": 1}], "next": "https://api.example.com/users?page=2"}
        page2 = {"results": [{"id": 2}], "next": None}
        mock_urlopen.side_effect = [
            _mock_response(page1),
            _mock_response(page2),
        ]

        ext = APIExtractor(url="https://api.example.com/users")
        records = list(ext.extract())
        assert len(records) == 2
        assert mock_urlopen.call_count == 2

    @patch("pipeflow.extractors.api.urllib.request.urlopen")
    def test_link_header_pagination(self, mock_urlopen: MagicMock) -> None:
        page1_data = [{"id": 1}]
        page2_data = [{"id": 2}]
        mock_urlopen.side_effect = [
            _mock_response(
                page1_data,
                headers={"Link": '<https://api.example.com/users?page=2>; rel="next"'},
            ),
            _mock_response(page2_data),
        ]

        ext = APIExtractor(url="https://api.example.com/users")
        records = list(ext.extract())
        assert len(records) == 2

    @patch("pipeflow.extractors.api.urllib.request.urlopen")
    def test_query_params(self, mock_urlopen: MagicMock) -> None:
        mock_urlopen.return_value = _mock_response([{"id": 1}])
        ext = APIExtractor(
            url="https://api.example.com/users",
            params={"status": "active"},
        )
        list(ext.extract())
        called_req = mock_urlopen.call_args[0][0]
        assert "status=active" in called_req.full_url

    @patch("pipeflow.extractors.api.urllib.request.urlopen")
    def test_custom_headers(self, mock_urlopen: MagicMock) -> None:
        mock_urlopen.return_value = _mock_response([{"id": 1}])
        ext = APIExtractor(
            url="https://api.example.com/users",
            headers={"Authorization": "Bearer token123"},
        )
        list(ext.extract())
        called_req = mock_urlopen.call_args[0][0]
        assert called_req.get_header("Authorization") == "Bearer token123"
