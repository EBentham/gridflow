"""The designated-date column name -> SQL type map (ADR-024, ADR-034 P-12).

A leaf module with no imports beyond ``typing``, so the NESO registry's
frozen-record validation (V-13) can read the map without importing
``gridflow.silver.base`` or the schema manifest. The manifest imports both
names back under its historical private spellings.

One name, one SQL type: ``gridflow_models``' manifest loader raises when a
designated date name carries two types across manifest entries, so a generated
family may reuse a name here only with the type recorded here.
"""

from __future__ import annotations

from typing import Literal

__all__ = ["DATE_COL_SQL_TYPES", "DateColSqlType"]

DateColSqlType = Literal["DATE", "TIMESTAMPTZ"]

DATE_COL_SQL_TYPES: dict[str, DateColSqlType] = {
    "settlement_date": "DATE",
    "gas_day": "DATE",
    # A calendar DATE, like `settlement_date` and unlike `timestamp_utc`:
    # NESO's daily wind availability is stated for a GB availability DAY, and
    # the derived instant lives in `timestamp_utc` (D-25).
    "availability_date": "DATE",
    # Calendar days of unit X's two activation families (ADR-037 P-12): the
    # CMP381/395 workbooks' `settlement_day` (an sp_pair date) and the phase-2
    # FFR ResultSummary archive's `date` (date_sp1).
    "settlement_day": "DATE",
    "date": "DATE",
    # The capacity-market register histories' reporting date (v0.22-K-CI): the `date_sp1`
    # anchor of the three change-log families, a calendar DATE like `date`.
    "change_date": "DATE",
    "timestamp_utc": "TIMESTAMPTZ",
    "implementation_datetime_utc": "TIMESTAMPTZ",
    "ingested_at": "TIMESTAMPTZ",
}
