# v0.22-H fixtures (system frequency)

Every `*.csv` here is cut from the 2026-10-08 swept bronze
`C:/gridflow-data/bronze/neso_data_portal/system_frequency/2026/10/08/` (read-only; a scratch script,
not committed, that reads each body or ZIP member line by line and keeps the named physical lines
byte for byte). No line is edited; CRLF terminators are kept, and `.gitattributes` (`*.csv -text`)
keeps them through git. Line numbers are the body's physical lines (header = line 1). ZIP bodies are
never committed: the tests rebuild a deflated one-member ZIP at test time from the member cut, under
the real member name.

| Fixture | Cut rule | Family | Resource id | Vendor filename | `ckan_last_modified` | Real body | Body bytes (real) | sha256 prefix (real body) |
|---|---|---|---|---|---|---|---:|---|
| `f-2014-1.csv` | header + lines 2-6 (first 5 records) | `system_frequency_dmy_csv` | 0cd6ff2e-62ca-4fbf-8417-18b3539530e6 | `f-2014-1.csv` | 2023-03-24T15:08:11.628448 | `raw_20261008T112747Z_0cd6ff2e-…_ec15f299.csv` | 74675888 | ec15f299 |
| `f-2014-10.csv` | header + lines 2-4 + both `26/10/2014 01:00:00` lines (2160002, 2163602) | `system_frequency_dmy_csv` | 32f82531-81ad-43b9-99a4-ca42b3d7aa59 | `f-2014-10.csv` | 2023-03-24T16:52:41.911424 | `raw_20261008T112815Z_32f82531-…_96c5213b.csv` | 74666907 | 96c5213b |
| `f-2015-3.csv` | header + lines 2-4 + lines 2422800-2422802 (the spring change) | `system_frequency_offset_csv` | 0f008751-3f0b-4353-9d4a-72f208221532 | `f-2015-3.csv` | 2023-03-24T13:07:55.362409 | `raw_20261008T112833Z_0f008751-…_9838902b.csv` | 90742824 | 9838902b |
| `f 2019 12.csv` | member header + lines 2-6 of the ZIP's one member `f 2019 12.csv` | `system_frequency_offset_zip` | f0933bdd-1b0e-4dd3-aa7f-5498df1ba5b9 | `f-2019-12.zip` | 2020-02-14T18:28:17.366499 | `raw_20261008T113117Z_f0933bdd-…_234eaecd.zip` | 10527508 | 234eaecd |
| `fNew 2020 1.csv` | member header + lines 2-6 of the ZIP's one member `fNew 2020 1.csv` | `system_frequency_gmt_zip` | e1eb079f-abab-47c8-b59d-d558b3399796 | `fnew-2020-1.zip` | 2020-06-11T10:56:50.039159 | `raw_20261008T113151Z_e1eb079f-…_1e558a26.zip` | 7489183 | 1e558a26 |
| `fnew-2023-10.csv` | header + lines 2-4 | `system_frequency_gmt_csv` | 530d18c9-01bc-4620-9911-0d03c7152081 | `fnew-2023-10.csv` | 2023-11-02T15:29:22.324236 | `raw_20261008T113444Z_530d18c9-…_7e40e83a.csv` | 74692280 | 7e40e83a |
| `fnew-2026-8.csv` | header + lines 2-4 | `system_frequency_gmt_csv` | 758fa76d-b645-4532-a532-ab49335ee4c3 | `fnew-2026-8.csv` | 2026-09-09T13:16:21.702788 | `raw_20261008T113718Z_758fa76d-…_510b37fd.csv` | 74697120 | 510b37fd |

Package `system-frequency-data` (id `cb1cc925-ecd8-4406-b021-3a3f368196e1`), every capture
`url_type: upload`, CKAN format `CSV` (the ZIP bodies too). Every body and member: header `dtm,f`,
CRLF, no BOM, strict UTF-8.

## What each cut carries

| Fixture | What it proves |
|---|---|
| `f-2014-1.csv` | R1: day-first, offset-less labels (`01/01/2014 00:00:00`), kept as raw strings |
| `f-2014-10.csv` | R1: the repeated `26/10/2014 01:00:00` (values 50.029 and 49.94), October 2014's duplicate key (H5) |
| `f-2015-3.csv` | R2: `2015-03-29 00:59:58 +0000`, `00:59:59 +0000`, `02:00:00 +0100`, the in-row offset across the spring change |
| `f 2019 12.csv` | R2 inside a ZIP member (`+0000`), the ZIP and CSV paths reading equal |
| `fNew 2020 1.csv` | R3 inside a ZIP member: naive `2020-01-01 00:00:00`, GMT-stated |
| `fnew-2023-10.csv` | R3 in October 2023 (still summer time on the 1st): `2023-10-01 00:00:00` is midnight UTC, no BST shift |
| `fnew-2026-8.csv` | R3's date-only midnight: first record `2026-08-01,49.931`, then full cells |

`base_pin.json` is not a vendor cut: it is the byte-unchanged golden written on the untouched base
(master `4c4b8ec`) by `tests/unit/_neso_h_pin.py`.
