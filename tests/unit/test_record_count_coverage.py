"""T3-f (D-18.1/P3-5/P3-8): AST coverage registry for every ``RawResponse(``
construction site under ``src/gridflow/connectors``.

Each site is classified STAMPED / POST_STAMPED / EXEMPT (D-18.1). The
pinned counts are the anti-drift invariant this module exists for: a new
construction, a new caller of a POST_STAMPED constructor, or a change to
either's count fails this test until the registry is deliberately updated --
the same idiom as the T1-q AST pin (D-21). This module also carries the
parser-level unit tests for the parse-once helpers (record_count_from,
count_timeseries_or_none) and a measured (not merely read) parse-count
assertion for Elexon's ``_fetch_date_period``.
"""

from __future__ import annotations

import ast
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

CONNECTORS_ROOT = Path(__file__).resolve().parents[2] / "src" / "gridflow" / "connectors"


@dataclass(frozen=True)
class _RegistryEntry:
    """One classified ``RawResponse(`` constructor function."""

    classification: str  # "STAMPED" | "POST_STAMPED" | "EXEMPT"
    construction_count: int
    reason: str = ""
    # POST_STAMPED only: {"relpath.py:caller_fn": expected_call_count}
    callers: dict[str, int] = field(default_factory=dict)


# Seed per §3.4 of the R2-C plan (audited reasons -- each says WHY the count
# is unavailable at that site).
REGISTRY: dict[str, _RegistryEntry] = {
    "elexon/client.py:_fetch_date": _RegistryEntry("STAMPED", 1),
    "elexon/client.py:_fetch_date_period": _RegistryEntry("STAMPED", 1),
    "elexon/client.py:_fetch_date_path": _RegistryEntry("STAMPED", 1),
    "elexon/client.py:_fetch_datetime_range": _RegistryEntry("STAMPED", 1),
    "elexon/client.py:_fetch_single": _RegistryEntry(
        "EXEMPT",
        1,
        reason="no existing parse on this path -- reference data, body never parsed",
    ),
    "entsoe/client.py:_raw_response_to_records": _RegistryEntry(
        "POST_STAMPED",
        2,
        reason=(
            "stamped by the caller (_fetch_document) before the extend, D-25 -- the "
            "constructor itself never parses the body"
        ),
        callers={"entsoe/client.py:_fetch_document": 2},
    ),
    "entsog/client.py:_fetch_one": _RegistryEntry(
        "EXEMPT",
        1,
        reason=(
            "200 bodies are never parsed on the fetch path; the empty convention is an "
            "HTTP 404 + 'No result found' body, already excluded by http_status < 400"
        ),
    ),
    "gie/client.py:_fetch_paginated": _RegistryEntry(
        "EXEMPT",
        1,
        reason="module parsers run on other paths; no parse at this construction site",
    ),
    "gie/client.py:_fetch_country": _RegistryEntry(
        "EXEMPT",
        1,
        reason="module parsers run on other paths; no parse at this construction site",
    ),
    "neso_data_portal/client.py:fetch": _RegistryEntry(
        "EXEMPT",
        1,
        reason=(
            "CSV body is handed to bronze unparsed; the D-36 admission parse is "
            "discarded and is not a row count of record"
        ),
    ),
    "neso/carbon_intensity.py:fetch": _RegistryEntry(
        "EXEMPT",
        1,
        reason="no parse on the fetch path -- the body is handed to bronze unparsed",
    ),
    "openmeteo/client.py:_fetch_location": _RegistryEntry(
        "EXEMPT",
        1,
        reason=(
            "no parse on the fetch path -- Open-Meteo is the review's positive control "
            "for 'genuinely no parse exists here'"
        ),
    ),
}


def _relpath(path: Path) -> str:
    return path.relative_to(CONNECTORS_ROOT).as_posix()


def _callee_name(func: ast.expr) -> str | None:
    """Return the bare name of a call target: ``foo(...)`` or ``self.foo(...)``."""
    if isinstance(func, ast.Name):
        return func.id
    if isinstance(func, ast.Attribute):
        return func.attr
    return None


def _has_keyword(call: ast.Call, name: str) -> bool:
    return any(kw.arg == name for kw in call.keywords)


class _ModuleWalker(ast.NodeVisitor):
    """Tracks the enclosing function for every ``RawResponse(`` construction
    and every call to a named POST_STAMPED constructor, within one module."""

    def __init__(self, watched_callees: frozenset[str]) -> None:
        self._stack: list[str] = []
        self._watched_callees = watched_callees
        self.constructions: list[tuple[str, ast.Call]] = []  # (enclosing_fn, call)
        self.calls: list[str] = []  # enclosing_fn, one entry per matched call

    def _enter_function(self, node: ast.FunctionDef | ast.AsyncFunctionDef) -> None:
        self._stack.append(node.name)
        self.generic_visit(node)
        self._stack.pop()

    def visit_FunctionDef(self, node: ast.FunctionDef) -> None:  # noqa: N802
        self._enter_function(node)

    def visit_AsyncFunctionDef(self, node: ast.AsyncFunctionDef) -> None:  # noqa: N802
        self._enter_function(node)

    def visit_Call(self, node: ast.Call) -> None:  # noqa: N802
        enclosing = self._stack[-1] if self._stack else "<module>"
        name = _callee_name(node.func)
        if name == "RawResponse":
            self.constructions.append((enclosing, node))
        if name is not None and name in self._watched_callees:
            self.calls.append(enclosing)
        self.generic_visit(node)


@dataclass
class _ConnectorsIndex:
    """Per-``"relpath.py:function"`` indices over the whole connectors tree."""

    constructions: dict[str, list[ast.Call]]
    call_counts: dict[str, int]


def _walk_connectors(watched_callees: frozenset[str]) -> _ConnectorsIndex:
    """Walk every ``.py`` file under the connectors tree once.

    Returns constructions and matched-call counts keyed by
    ``"relpath.py:function"`` (re-keyed per file so two files with a
    same-named function never collide).
    """
    keyed_constructions: dict[str, list[ast.Call]] = {}
    keyed_calls: dict[str, int] = {}

    for path in sorted(CONNECTORS_ROOT.rglob("*.py")):
        relpath = _relpath(path)
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        walker = _ModuleWalker(watched_callees)
        walker.visit(tree)
        for fn_name, call in walker.constructions:
            keyed_constructions.setdefault(f"{relpath}:{fn_name}", []).append(call)
        for fn_name in walker.calls:
            key = f"{relpath}:{fn_name}"
            keyed_calls[key] = keyed_calls.get(key, 0) + 1

    return _ConnectorsIndex(constructions=keyed_constructions, call_counts=keyed_calls)


def test_every_raw_response_construction_site_is_classified() -> None:
    """Every ``RawResponse(`` construction, grouped by its enclosing function,
    is classified STAMPED / POST_STAMPED / EXEMPT with a pinned count.

    A new construction site, a removed one, or a count drift fails here --
    the anti-drift invariant (D-18.1/P3-8).
    """
    index = _walk_connectors(frozenset({"_raw_response_to_records"}))
    keyed = index.constructions

    unclassified = sorted(set(keyed) - set(REGISTRY))
    assert not unclassified, (
        f"unclassified RawResponse( construction site(s): {unclassified} -- add a "
        "REGISTRY entry (STAMPED/POST_STAMPED/EXEMPT) with a reason"
    )

    stale = sorted(set(REGISTRY) - set(keyed))
    assert not stale, (
        f"REGISTRY entry has no matching construction site (stale, site removed or "
        f"renamed): {stale}"
    )

    for key, entry in REGISTRY.items():
        calls = keyed[key]
        assert len(calls) == entry.construction_count, (
            f"{key}: expected {entry.construction_count} RawResponse( construction(s), "
            f"found {len(calls)} -- update REGISTRY if this is a deliberate change"
        )
        if entry.classification == "STAMPED":
            for call in calls:
                assert _has_keyword(call, "record_count"), (
                    f"{key}: classified STAMPED but a construction site is missing "
                    "record_count= -- either stamp it or reclassify"
                )
        else:
            # EXEMPT and the POST_STAMPED constructor's OWN call sites must NOT
            # stamp at construction -- POST_STAMPED is stamped externally by the
            # caller (D-25), and EXEMPT has no count to stamp with.
            for call in calls:
                assert not _has_keyword(call, "record_count"), (
                    f"{key}: classified {entry.classification} but a construction "
                    "site now passes record_count= -- reclassify to STAMPED"
                )


def test_post_stamped_callers_and_call_counts_are_pinned() -> None:
    """For every POST_STAMPED entry, the caller SET and each caller's
    call-site COUNT are pinned (D-18.1/P3-5): a new caller, a removed caller,
    or a call-count drift fails until re-classified."""
    post_stamped = {k: v for k, v in REGISTRY.items() if v.classification == "POST_STAMPED"}
    assert post_stamped, "expected at least one POST_STAMPED entry (entsoe, D-18.1)"

    for key, entry in post_stamped.items():
        _, fn_name = key.split(":", 1)
        index = _walk_connectors(frozenset({fn_name}))

        # `_walk_connectors` was scoped to `{fn_name}`, so every entry in
        # `call_counts` IS a call to this specific constructor.
        actual = dict(index.call_counts)
        assert actual == entry.callers, (
            f"{key}: caller set/count mismatch -- expected {entry.callers}, found "
            f"{actual}. A new caller or a call-count drift must be re-classified "
            "(D-18.1)."
        )


# ---------------------------------------------------------------------------
# Parser-level unit tests: the parse-once helpers behind the stamping (T3-d/e).
# ---------------------------------------------------------------------------


class TestElexonParseOnceHelpers:
    """Direct unit tests for ``pagination_from`` / ``record_count_from``."""

    def test_record_count_from_counts_a_populated_data_list(self) -> None:
        from gridflow.connectors.elexon.parsers import record_count_from

        assert record_count_from({"data": [{"x": 1}, {"x": 2}]}) == 2

    def test_record_count_from_is_zero_for_a_parsed_empty_list(self) -> None:
        from gridflow.connectors.elexon.parsers import record_count_from

        assert record_count_from({"data": []}) == 0

    def test_record_count_from_is_none_on_a_parse_failure(self) -> None:
        """D-8: a truncated/malformed JSON body -- ``parse_json_response``
        swallows the error and returns ``{}`` -- must stamp None, never 0."""
        from gridflow.connectors.elexon.parsers import parse_json_response, record_count_from

        parsed = parse_json_response(b'{"data": [{"x": 1}')  # truncated
        assert parsed == {}
        assert record_count_from(parsed) is None

    def test_record_count_from_is_none_for_unexpected_shape(self) -> None:
        from gridflow.connectors.elexon.parsers import record_count_from

        assert record_count_from({"data": "not-a-list"}) is None
        assert record_count_from([]) is None  # type: ignore[arg-type]

    def test_get_pagination_info_contract_unchanged(self) -> None:
        """``get_pagination_info`` is now a thin wrapper over
        ``pagination_from(parse_json_response(body))`` -- contract unchanged."""
        import json

        from gridflow.connectors.elexon.parsers import get_pagination_info

        body = json.dumps({"metadata": {"page": 2, "totalPages": 5}}).encode()
        assert get_pagination_info(body) == (2, 5)

    def test_fetch_date_period_parses_each_body_exactly_once(self) -> None:
        """Elexon net-parse-count assertion (measured, not merely read): after
        T3-d, ``_fetch_date_period`` calls ``parse_json_response`` exactly
        ONCE per HTTP response (previously twice: an explicit ``json.loads``
        for the page-1-empty check, plus a second parse inside
        ``get_pagination_info``)."""
        import asyncio

        import gridflow.connectors.elexon.client as client_module
        from gridflow.config.settings import DatasetConfig, SourceConfig
        from gridflow.connectors.elexon.client import ElexonConnector
        from gridflow.connectors.elexon.endpoints import ENDPOINTS

        call_count = [0]
        real_parse = client_module.parse_json_response

        def _counting_parse(body: bytes) -> dict[str, Any]:
            call_count[0] += 1
            return real_parse(body)

        config = SourceConfig(
            base_url="https://data.elexon.co.uk/bmrs/api/v1",
            rate_limit_per_second=1000,
            timeout=5,
            datasets={"pn": DatasetConfig()},
        )
        endpoint = ENDPOINTS["pn"]

        class _FakeRaw:
            content = (
                b'{"data": [{"settlementPeriod": 1}], "metadata": {"page": 1, "totalPages": 1}}'
            )
            headers = {"content-type": "application/json"}
            status_code = 200
            url = "https://data.elexon.co.uk/bmrs/api/v1/datasets/PN"

        async def _fake_request(self: object, path: str, params: dict[str, Any]) -> _FakeRaw:
            return _FakeRaw()

        connector = ElexonConnector(config)
        real_request = client_module.ElexonConnector._request
        try:
            client_module.parse_json_response = _counting_parse
            client_module.ElexonConnector._request = _fake_request  # type: ignore[method-assign]
            responses = asyncio.run(
                connector._fetch_date_period(
                    "pn", endpoint, __import__("datetime").date(2024, 1, 15), max_periods=1
                )
            )
        finally:
            client_module.parse_json_response = real_parse
            client_module.ElexonConnector._request = real_request  # type: ignore[method-assign]

        assert len(responses) == 1
        assert responses[0].record_count == 1
        assert call_count[0] == 1, (
            f"expected exactly one parse_json_response call per HTTP response, got {call_count[0]}"
        )


class TestEntsoeParseOnceHelpers:
    """Direct unit tests for ``count_timeseries_or_none``."""

    def test_counts_timeseries_elements_namespace_agnostic(self) -> None:
        from gridflow.connectors.entsoe.client import count_timeseries_or_none

        xml = b'<root xmlns="urn:x"><TimeSeries/><TimeSeries/></root>'
        assert count_timeseries_or_none(xml) == 2

    def test_is_zero_for_a_valid_empty_document(self) -> None:
        from gridflow.connectors.entsoe.client import count_timeseries_or_none

        assert count_timeseries_or_none(b"<root></root>") == 0

    def test_is_none_for_malformed_xml(self) -> None:
        """#5: malformed XML must stamp None, never 0 (D-8)."""
        from gridflow.connectors.entsoe.client import count_timeseries_or_none

        assert count_timeseries_or_none(b"<root><unclosed>") is None

    def test_is_none_for_empty_body(self) -> None:
        from gridflow.connectors.entsoe.client import count_timeseries_or_none

        assert count_timeseries_or_none(b"") is None
