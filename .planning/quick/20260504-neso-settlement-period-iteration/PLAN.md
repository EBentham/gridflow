---
slug: neso-settlement-period-iteration
status: complete
created: 2026-05-04
---

# NESO Settlement Period Iteration

Fix NESO `intensity_period` fetching so each requested settlement date queries
all valid GB settlement periods, then prove bronze and silver contain the full
period set.

## Tasks

- Add endpoint metadata for settlement-period fan-out.
- Iterate all valid GB settlement periods per requested date, including DST
  short and long settlement days.
- Preserve requested data dates for bronze partitioning and silver transform.
- Add mocked regression tests and run live API-to-silver verification.
