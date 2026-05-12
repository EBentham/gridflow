# Phase L1 Validation Strategy

**Phase:** L1
**Created:** 2026-05-04

## Validation Architecture

L1 is a contract phase. Validation should prove that endpoint catalog,
connector metadata, listing-derived inventory planning, and requirement
traceability agree before runtime fetch behavior changes.

## Required Checks

- Unit tests for catalog parsing and active/deferred status validity.
- Unit tests comparing `docs/gie_agsi_endpoint_catalog.yaml` to `ENDPOINTS` in `src/gridflow/connectors/gie/endpoints.py`.
- Unit tests for listing payload parsing using a fixture with multiple companies and facilities.
- Unit tests for expected request counts for aggregate, country, company, and facility scopes.
- Unit tests for date-window planning, including exact-day and multi-day ranges.

## Must Not Rely On

- Live API availability for the core L1 gate.
- Current production listing size.
- Existing hard-coded country arrays as the only inventory source.

