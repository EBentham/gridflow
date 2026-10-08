# ADR-037 — NESO container readers, child inventories and the COVERED harness

**Status:** proposed
**Date:** 2026-10-08
**Phase:** v0.22 unit X (Excel and ZIP reader)
**Amends:** ADR-034 (reader specs on the record, the `child_crc32` provenance column, V-12
extended, V-14/V-15/V-15b; `stale_covered` re-derived from proof inputs; the registry-side
inventory digest and the capture-id COVERED evidence are deleted).
**Cross-references:** ADR-033 (registry, upload leg), ADR-034 (generic engine, completion
ledger, reconcile and drain), ADR-036 (profiler, record shape).

## Context

After unit E, 131 registry resources were `HOLD(X-R)`: workbooks (XLSX 50, XLSM 23), ZIP
archives (19) and CSV-declared ZIPs (39 system-frequency bodies). Unit X-R's research
proposed a child inventory and a disposition per resource, nominated value-equivalence
(`COVERED`) pairs, and measured the bodies. Three facts shaped this unit:

- Python's `zipfile` trusts declared sizes. A deflated entry whose headers declare a
  prefix length and the prefix's CRC reads back as the prefix, with no error, and
  `testzip()` is clean.
- B's engine exempts **every** capture of a `COVERED` resource from transform, not just
  the newest. B's evidence pinned capture ids and a digest of `(child, disposition)`
  pairs, so installing a grant changed the digest and older captures were never proved.
- No nomination survives scrutiny today: 12 FFR pairs fail the vintage leg, 16 have no
  record on either side, the GSP member's covering CSV was published 129.559 s later, and
  balancing-1 overlaps on two differing records only. X therefore commits **zero**
  `COVERED` grants (C-1). It ships the harness, proven on synthetic pairs.

## Decision

**P-1 — the value engine.** `fastexcel` (calamine) is the one new dependency. Cell values
are read only through `pl.read_excel(engine="calamine", infer_schema_length=0,
drop_empty_rows=False, drop_empty_cols=False, raise_if_empty=False)` with an explicit
`header_row`, `use_columns` and `n_rows`, so every column is `Utf8` and the record's strict
casts own typing. Workbook structure (sheet map, merges, error cells, formula caches,
populated cells) is read with stdlib `xml.etree` over verified parts.

**P-2 — reader specs on the record.** `SchemaRecord` gains `xlsx: XlsxSpec | None`
(`header_row`, `columns` `A:J`, optional `last_row`) and `zip_member: ZipMemberSpec | None`
(`member_pattern`, `inner` `csv|xlsx`). **V-14:** the specs must match `reader`. Sheet and
member are not record fields; they are the child id.

**P-3 — child identity.** `containers.list_children` is the one definition of a body's
children: a workbook's sheets in `workbook.xml` order; otherwise every non-directory entry,
with a workbook member (sniffed by content) expanded as `member::sheet`. Nested ZIPs are
listed, never expanded. A `::` in a member name, a repeated sheet name or a repeated child
id is refused.

**P-4 — the container gate.** Every ZIP that X touches (bodies, workbook parts, GIS members
to depth 2) is opened by `open_container` and read only by `read_entry`. `open_container`
holds the only `zipfile` constructor in X's modules, refuses repeated entry names and caps
declared sizes before any decompression (1,000 entries, 256 MiB per entry, 1 GiB total).
`read_entry` decompresses with zlib directly and proves the local header (signature,
method, no encryption, local/central agreement when bit 3 is clear), the exact compressed
slice, stream completion (`eof`, no unused data, no unconsumed tail), the exact length and
the CRC. Workbooks are verified entry by entry before calamine sees them; `<!DOCTYPE` in a
workbook XML part is refused. No X module calls `ZipFile.read/open/extract*/testzip`;
a test fixture forbids it at runtime and a grep pins it statically.

**P-5 — inventory enforcement.** The `xlsx` and `zip_member` readers resolve the body's
resource from its sidecar through the call-time registry seam and require
`set(list_children(body))` to equal the registry inventory; any difference is
`ContainerInventoryError`. **V-15** (load): a childless resource's `SILVER(k)` needs
`reader == "csv"`; a child's `SILVER(k)` needs a container reader whose shape fits the
child id. **V-15b:** a resource-level `SILVER(k)` with children has every SILVER child
targeting `k`. The audit CLI (`python -m gridflow.silver.neso_data_portal.containers audit`)
compares every inventory with its newest capture, read-only.

**P-6 — the `xlsx` block.** Rules (a)–(g), first failure wins, as `XlsxBlockError` naming
the rule and cell: (a) header cell empty, non-string or repeated; (b) a merged range in the
block; (c) an error cell or uncached formula; (d) an empty data row; (e) a populated cell
outside the columns; (f) row `last_row + 1` populated; (g) calamine's header or row count
differing from the stdlib parse. The child CRC is the worksheet part's.

**P-7 — the `zip_member` reader.** The member must fullmatch `member_pattern`. `inner="csv"`
goes through `_csv_member_table`, which mirrors `read_csv_body` line for line;
`inner="xlsx"` goes through P-4's workbook rule and P-6. The child CRC is the member's.

**P-8/P-9 — dispositions.** All 131 `HOLD(X-R)` resources carry committed inventories.
Tallies: SILVER 1,249, HOLD 73, DOC 43, GIS 20. Containers whose proposed families have no
record are `HOLD(batch)` with the proposed key (renamed to fit `KEY_PATTERN`) in the reason;
V-11 is not relaxed. No `X-R` unit survives.

**P-10 — sibling-fed families.** A family no resource names is sibling-fed; its ingest
returns before any request. A committed-registry test (V-16) requires every recorded
sibling-fed family to list a sibling.

**P-11 — member provenance.** Container outputs carry `child_crc32` (`Int64`) after
`child_id`: the verified CRC of the entry the rows came from. `child_crc32` is reserved.
CSV records gain nothing.

**P-12 — the activation records.** `current_bsuos_cap_adjustments` (CMP381/395 workbooks,
`xlsx`, header row 8, `A:J`, `sp_pair`, keyed on day, period and run type) and
`ffr_phase2_result_summary_archive` (the ResultSummary ZIP, `zip_member` over CSV,
`date_sp1`, record eligibility held for AS-5: Date and EFA semantics are undocumented).

**P-13 — one proof-input value, one fingerprint.** `CoveredDisposition` is
`{kind, by, key, evidence}` and `CoveredEvidence` is `{fingerprint, components}`.
`equivalence.gather_inputs` builds the one `ProofInputs` value: harness versions, both
legs (derived from the grant's registry position), the covered resource's sorted child
ids, both records, and both scopes (each capture's body SHA-256 plus the sidecar metadata
its record reads). `comparison_components` digests each field by iterating the dataclass
fields, so a proof input that is not a component cannot exist; the fingerprint digests the
components. Evidence and dispositions are never inputs, so installing a grant changes
nothing (**I-PROOF**), and evidence copied to another site does. `gather_inputs` refuses
when the reader seam's registry gives the site different children. **V-12** (after load):
a grant needs evidence and a `key` that is a recorded tabular family equal to, or listing
as a sibling, the covered resource's family; `by` is another, childless `SILVER` resource.
`reconcile._stale_covered` re-derives the components at the reconcile cutoff and reports
every component that differs, an empty scope, or a fingerprint that does not match its
components; the drain never clears it.

**P-14 — the harness and its scope (I-SCOPE).** `generic.resource_of` is extracted from
`families_of` unchanged, and the scope uses it: every usable capture in every registry
family directory that resolves to the leg's resource, up to the cutoff, so the proof covers
the whole exemption. A covered capture is matched by a covering capture when (i) the
projections (record columns minus provenance) are equal multisets, (ii) both scopes type
cleanly, and (iii) the covering vintage is no later than the covered one. A grant needs
every covered capture matched. The CLI prints the JSON proof (exit 0 grant, 1 refusal,
2 usage). A grant enters the registry only with a committed test that re-proves it on byte
copies.

**P-15 — fixtures on disk.** `tests/fixtures/neso_data_portal/containers/` holds byte
copies of four real bronze bodies (CMP381 II workbook, TR129 XLSM, the ResultSummary ZIP,
the TNUoS generation-zones shapefile ZIP); `PROVENANCE.md` gives each source path, SHA-256
and the sidecar values the tests use, and a test re-hashes every fixture. Negative cases
come from a stdlib workbook builder and ZIP corrupters (repeated name, bad CRC, the
false-size prefix construction). Every X test module runs under `forbid_zipfile_reads`,
which fails any `ZipFile.read/open/extract*/testzip` call made from a NESO module.

**P-16 — GIS facts.** `connectors/neso_data_portal/gis.py` states per layer the feature
count, bounding box and CRS of every `GIS` resource's newest capture (GeoJSON via `json`;
GPKG via in-memory `sqlite3.deserialize` with bound values and an identifier regex on the
one interpolated table name; shapefiles from `.shx` length, `.shp` header and `.prj`;
nested archives to depth 2 through P-4). The committed `docs/neso_data_portal/gis-facts.json`
is checked with `--check`.

**P-17 — the CSV path is byte-unchanged.** `ENGINE_VERSION` stays `"1"`; `read_csv_body`,
`read_csv_bronze_body` and `finish_capture` are not edited; B's goldens and E's pilot
fixtures are byte-identical.

## Failure modes

A grant exempts every capture of its site, so the mechanism owns a silent-loss class. Each
interruption is covered: a new covered or covering capture, a record change, a moved
target, an inventory change or a harness bump each change exactly one component and report
`stale_covered` (F-1–F-5, F-9); a dropped covering resource fails V-12 and an uncaptured one
reports an empty scope (F-7); disposition edits and installs change nothing (F-15); an
unmatched historical capture refuses the proof (F-16), including one outside its family's
directory (F-17); an earlier reconcile cutoff fails closed (F-18); a seam/registry
disagreement refuses before any read (F-19). Container faults fail the capture loud with no
output and no completion: an inventory change (F-10), a lying or truncated entry (F-11), a
failing child (F-12), a shadowed entry or repeated sheet (F-14). Interrupted transforms
recover through B's drain (F-13).

## Consequences and residuals

- **R-1** Metadata that changes with no new capture stays undetectable (ADR-034 FM-14).
- **R-2** Composite merged headers (FFR, ORPS) and multi-block sheets are rejected loudly by
  P-6; flattening is a new recipe for AS-5, AS-3 and BAL-2.
- **R-3** `system_frequency` mixes CSV and ZIP bodies under one family; unit H chooses a
  per-body reader or a split before its record can load.
- **R-4** A covering resource wider than the covered child never grants; the rows stay
  SILVER (a duplicate, not a loss).
- **R-5** An unusable sidecar belongs to no scope; B's reconcile reports it `failed`.
- **R-6** calamine re-decompresses verified parts; a misread is caught by P-6(g).
- **R-7** Any new covering capture with new bytes voids a grant, forcing re-proof; it
  never hides a row.
