"""HTTP API extractor using urllib.request with pagination support."""

from __future__ import annotations

import json
import urllib.request
import urllib.parse
from typing import Any, Iterator

from pipeflow.types import Record


class APIExtractor:
    """Extract records from an HTTP API endpoint.

    Supports pagination via:
    - Link header (rel="next")
    - Offset-based (offset/limit query params)
    """

    def __init__(
        self,
        url: str,
        headers: dict[str, str] | None = None,
        params: dict[str, str] | None = None,
        pagination: dict[str, Any] | None = None,
    ) -> None:
        self.url = url
        self.headers = headers or {}
        self.params = params or {}
        self.pagination = pagination  # e.g. {"type": "offset", "limit": 100}

    def extract(self) -> Iterator[Record]:
        """Yield records, following pagination if configured."""
        url = self._build_url(self.url, self.params)

        while url:
            data, next_url = self._fetch_page(url)
            if isinstance(data, list):
                yield from data
            elif isinstance(data, dict):
                # Support {"results": [...]} or {"data": [...]} patterns
                for key in ("results", "data", "items", "records"):
                    if key in data and isinstance(data[key], list):
                        yield from data[key]
                        break
                else:
                    yield data
            url = next_url

    def _build_url(self, base_url: str, params: dict[str, str]) -> str:
        if not params:
            return base_url
        sep = "&" if "?" in base_url else "?"
        return base_url + sep + urllib.parse.urlencode(params)

    def _fetch_page(self, url: str) -> tuple[Any, str | None]:
        """Fetch a single page. Returns (data, next_url)."""
        req = urllib.request.Request(url, headers=self.headers)
        with urllib.request.urlopen(req) as response:
            data = json.loads(response.read().decode("utf-8"))
            next_url = self._get_next_url(response, data, url)
        return data, next_url

    def _get_next_url(self, response: Any, data: Any, current_url: str) -> str | None:
        """Determine next page URL from response."""
        # Check Link header first
        link_header = response.headers.get("Link", "")
        if link_header:
            next_link = self._parse_link_header(link_header)
            if next_link:
                return next_link

        # Check for next URL in response body
        if isinstance(data, dict):
            for key in ("next", "next_url", "next_page"):
                if key in data and data[key]:
                    return str(data[key])

        # Offset-based pagination
        if self.pagination and self.pagination.get("type") == "offset":
            limit = self.pagination.get("limit", 100)
            if isinstance(data, list) and len(data) >= limit:
                parsed = urllib.parse.urlparse(current_url)
                params = dict(urllib.parse.parse_qsl(parsed.query))
                current_offset = int(params.get("offset", "0"))
                params["offset"] = str(current_offset + limit)
                params["limit"] = str(limit)
                new_query = urllib.parse.urlencode(params)
                return urllib.parse.urlunparse(parsed._replace(query=new_query))

        return None

    @staticmethod
    def _parse_link_header(header: str) -> str | None:
        """Parse Link header for rel='next'."""
        for part in header.split(","):
            part = part.strip()
            if 'rel="next"' in part or "rel='next'" in part:
                url_part = part.split(";")[0].strip()
                if url_part.startswith("<") and url_part.endswith(">"):
                    return url_part[1:-1]
        return None
