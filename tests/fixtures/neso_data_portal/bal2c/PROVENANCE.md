# Provenance: AAHEDC and TNUoS tariff fixtures (v0.22-K-BAL-2c)

Every fixture is a cut of a newest-capture body under
`C:/gridflow-data/bronze/neso_data_portal/<family>/2026/10/08/` (swept 2026-10-08), read-only. A cut
keeps the original header and the original bytes of each kept line (a BOM stays; the AAHEDC body
has none, the eight TNUoS bodies have one); only the line selection is ours. Every original is
CRLF; `git` normalises the committed line endings, so the tests rebuild CRLF for all nine. The
"sha8" is the first eight hex digits of the original body's SHA-256, the suffix of the bronze file
name. "Lines" are 1-based line numbers of the original body (1 is the header). Nothing is repaired
or re-decoded.

| Fixture | Family | Resource id | Vendor file name | sha8 | Original rows | Lines kept |
|---|---|---|---|---|---:|---|
| `aa.csv` | `aahedc_tariffs` | `ffd29cc8-3c55-4e83-aa0e-73212d4fedba` | `aahedc-tariffs-for-dataportal-2026-07.csv` | `2ebbf27d` | 9 | 1-10 (the whole body: 6 Final, 3 Draft; no BOM) |
| `ee.csv` | `tnuos_tariffs_embedded_export` | `af4501a0-6b96-4088-926c-7c7b0c499b08` | `tnuos_embeddedexport.csv` | `b17c3bfa` | 602 | 1-3 (Forecast), 16-17 (Final), 30-31 (Draft); `PhasedResidual` zero |
| `hh.csv` | `tnuos_tariffs_half_hourly_hh_demand` | `6abe8416-149c-4c78-b65b-059eea19957a` | `tnuos_demand_hh.csv` | `b11b3c76` | 602 | 1-3, 16-17, 30-31 (same stage layout); `HH_SGD_Adjustment` zero |
| `nh.csv` | `tnuos_tariffs_non_half_hourly_nhh_demand` | `4e8dd130-5283-4f22-8c05-84e4d3280eb3` | `tnuos_demand_nhh.csv` | `50e19a27` | 602 | 1-3, 16-17, 30-31 (same stage layout); `NHH_Zonal` all zero |
| `og.csv` | `tnuos_tariffs_onshore_generator` | `ffc0adb8-5426-474e-94a3-a5f3f01a0cdb` | `tnuos_generation_onshore.csv` | `bf08ecbd` | 1,161 | 1-3 (Forecast), 29-30 (Final), 56-57 (Draft); `SmallGenDiscount` zero |
| `of.csv` | `tnuos_tariffs_local_offshore_generator` | `98569a29-940d-4bb7-8dc4-01e46187039a` | `tnuos_generation_offshore.csv` | `71f94175` | 1,346 (113 wholly blank) | 1-3 (Forecast), 37-38 (Final), 72-73 (Draft), 1235-1236 (two wholly blank `,,,,,,` rows); `ETUoS` zero (`0` and `0.000000` spellings) |
| `ls.csv` | `tnuos_tariffs_local_substation_tariff` | `e697e9be-c068-482d-ae35-01f60bcb6700` | `local-substation-tnuos-tariff.csv` | `8178f4bc` | 84 (48 wholly blank) | 1-3 (Forecast), 14-15 (Final), 26-27 (Draft), 38-39 (two wholly blank rows) |
| `td.csv` | `tnuos_tariffs_transmission_demand` | `dcca94fd-343e-4d4e-8c5d-66009dec4ad3` | `tnuos_tdr-tariffs.csv` | `595c0a59` | 462 | 1-3 (Forecast), 24-25 + 47 (Final; line 47 is the `Unmetered` band), 45-46 (Draft); header bytes `c3 82 c2 a3` (source mojibake) |
| `lc.csv` | `tnuos_tariffs_onshore_local_circuit` | `01f36b32-4ad3-47fa-a645-7f7a47f1b0ae` | `tnuos_onshore_local_circuit.csv` | `1a103d78` | 4,647 | 1-3 (Forecast), 22 and 86 (space-ended `CHLEANSAID `, `Millennium Wind `), 111-112 (Final), 206-207 (Draft), 299-300 (literal `2025/09/12` publication date), 1709 and 1885 (`"Fasque<TAB>"`, quoted by the vendor; tariff zero) |

The sidecar facts the tests write (package slug and id, resource name, vendor file name,
`ckan_last_modified`, the `written_at` capture time) are the originals', in
`test_neso_bal2c_records.py::CAPTURES`.

The row totals the records must reproduce (the seat's full-body check, RULINGS 692,
`BAL-2c-BRONZE-CHECK.json`; the bodies are not re-scanned): AAHEDC 9; embedded export 602; HH demand
602; NHH demand 602; onshore generator 1,161; offshore generator 1,233 (1,346 less 113 wholly blank);
local substation 36 (84 less 48 wholly blank); TDR 462; local circuit 4,647; 9,354 in all.

## Spec deviation: how the wholly blank rows leave the load

K-BAL-2c-SPEC says the 113 + 48 wholly blank rows are "excluded by the non-nullable key columns
with a tally". The existing mechanism is different: `silver/csv_bronze.read_csv_bronze_body` drops a
row whose every cell is empty before typing and logs the count at INFO ("dropped N blank row(s)");
those rows never reach the typed frame, so the completion's `rows_excluded` is 0 for them. A partly
blank row (a blank key cell with other content) does reach typing and is excluded by the
non-nullable key column with a tally. The kept rows are the same either way (1,233 and 36); the
tests assert the real mechanism for both cases. No engine change was made.

## Open TODOs the records carry (the record model has no notes field)

- TARIFF-ISSUE-AVAILABILITY (all nine): each body mixes Forecast / Draft / Final rows (AAHEDC
  Draft / Final only) with no publication instant. What was available when, and how mixed stages
  are treated, is unknown; a publication date alone does not establish availability at midnight.
  The stage labels are kept as supplied; a Final row is never relabelled a forecast.
- TDR-UNMETERED-UNIT (`tnuos_tariffs_transmission_demand`): ordinary bands are GBP/(site day), the
  unmetered band's unit conflicts (dictionary: p/kWh per day; final tariff statement: p/kWh/annum in
  its summary, p/kWh in its detailed table). The value column `tdr_tariff` carries no unit and no
  conversion; the conflict is in the hold question.
- CIRCUIT-PUBLICATION-DATE-CAST (`tnuos_tariffs_onshore_local_circuit`): one resource holds
  3,677 `DD/MM/YYYY` and 970 literal `2025/09/12` publication dates, so no single format and no
  `formats_by_filename` entry can apply. `published_date` stays the raw string, key included; a
  typed date waits for an existing faithful casting capability or a vendor-normalised body.
- The 78 local-circuit `Substation_Name` cells with a trailing space (73) or tab (5, vendor-quoted)
  are kept byte for byte (a key column is never trimmed, V-19); whether they are meaningful, or NESO
  should republish them trimmed, is in the hold question.
- Local substation `Tariff`: the header carries no unit. GBP/kW comes from the vendor five-year
  tariff statement's local-substation table (K-BAL-2-FACTS section 6 T02, W09, NESO document
  `367801`), not from a neighbouring family; the silver name is `tariff_gbp_per_kw`.
- Annual unit: NESO describes annual charging liabilities, but the columns declare GBP/kW; GBP/kW is
  kept and never rewritten as GBP/kW/year (FACTS section 6).
- Vendor observation, not acted on (data semantics are out of scope): in the newest AAHEDC body's
  last row (Final 2027, 15/07/2026) the `Shetland` cell is 0.031503 and the `excluding the Shetland
  Assistance Amount` cell 0.012766; in every other row (e.g. Draft 2027: 0.012766 and 0.031435) the
  Shetland cell is the smaller one. The two look swapped. Loaded as supplied.
