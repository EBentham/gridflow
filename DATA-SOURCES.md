# Data sources and their terms

gridflow fetches public market data from six vendors, through the eight sources in its
`sources.yaml`. For each source this page says what gridflow fetches, which API key it
needs, the vendor's terms and the attribution they ask for, and whether the files under
`tests/fixtures/` hold real vendor responses.

Every terms page linked here was read on 1 November 2026. Vendors change their terms
without notice, so read the linked page again before you publish data. This page is a
checklist, not legal advice.

**If you share data you fetched with gridflow, carry the vendor's attribution with it.**
gridflow's Apache-2.0 licence covers its code only; the data stays under each vendor's terms.

## At a glance

| Source | Key | Terms | Real responses in `tests/fixtures/` | Sharing those responses |
|---|---|---|---|---|
| Elexon BMRS | none | [BMRS open data licence][elexon-terms] | none (synthetic) | no real responses |
| ENTSO-E | `ENTSOE_API_KEY` | [Transparency Platform terms][entsoe-terms] | 1 of 36 XML files | unverified |
| ENTSO-G | none | [ENTSOG Transparency Platform terms][entsog-terms] | none (synthetic) | no real responses |
| GIE AGSI+ and ALSI | `GIE_API_KEY` | [AGSI][gie-agsi] and [ALSI][gie-alsi] data-use rules | 1 of 8 files may be real | allowed |
| NESO Carbon Intensity | none | [CC BY 4.0][neso-ci-home] plus [terms of use][neso-ci-terms] | 2 of 3 files | unclear |
| NESO Data Portal | none | [NESO Open Data Licence v1.0][neso-dp-terms] | most of 458 data files | allowed |
| Open-Meteo | none | [CC BY 4.0 data][om-licence], [free API for non-commercial use][om-terms] | none (synthetic) | no real responses |

"Allowed", "unclear" and "not allowed" are used only where the vendor's terms say so;
"unverified" means the deciding page couldn't be read.

## Elexon BMRS (`elexon`)

- **Fetches:** 33 datasets from the Insights Solution API at `data.elexon.co.uk/bmrs/api/v1`:
  system prices, bid-offer acceptances, balancing services adjustments, frequency,
  generation by fuel, demand and wind forecasts, physical notifications, REMIT messages,
  market depth and BM unit reference data.
- **Key:** none.
- **Terms:** [Licence to use BMRS open data][elexon-terms] (no version or date on the page).
  It applies to anyone who is not a BSC Party, and it allows copying, publishing, adapting
  and commercial use. Attribution, with a link to the licence where possible:
  "Contains BMRS data © Elexon Limited copyright and database right [year]".
- **Not settled:** the licence defines BMRS data as the data on the Insights Solution
  website and doesn't name the API that gridflow reads. The API serves the same data, so it
  is probably covered.
- **Fixtures:** all 25 files in `tests/fixtures/elexon/` are synthetic (round values,
  sequential ids).

## ENTSO-E Transparency Platform (`entsoe`)

- **Fetches:** 47 datasets from the REST API at `web-api.tp.entsoe.eu`: day-ahead prices,
  load and load forecasts, generation by type and by unit, wind and solar forecasts,
  cross-border flows, outages, transfer capacities and auctions, congestion management,
  imbalance and balancing data.
- **Key:** `ENTSOE_API_KEY`, free. Register on the [Transparency Platform][entsoe-tp], then
  ask ENTSO-E for RESTful API access and generate a token under My Account
  ([ENTSO-E's how-to][entsoe-token]).
- **Terms:** [General terms and conditions for the use of the Transparency
  Platform][entsoe-terms], approved 8 November 2018. You must name the ENTSO-E Transparency
  Platform as the source of the data. Data may also be under the copyright of the TSO that
  published it: data on ENTSO-E's list of freely reusable data can be reused without asking
  that owner, and you must check the list before each reuse.
- **Not settled:** the list is on [ENTSO-E's help site][entsoe-reuse-list], which refused
  automated reading on 1 November 2026, so it hasn't been checked here.
- **Fixtures:** 35 of the 36 XML files in `tests/fixtures/entsoe/` are synthetic (their
  document ids start `fixture-`). One is real:
  `balancing_energy_bids_a37_reservebid_de_20260601.xml`, a three-series cut of a live A37
  balancing-bid response for Germany on 1 June 2026. `parser_baseline.json` is gridflow's own
  parser output. **Sharing:** unverified, until that dataset is found on the reuse list.

## ENTSO-G Transparency Platform (`entsog`)

- **Fetches:** 33 datasets from the API at `transparency.entsog.eu/api/v1`: physical flows,
  nominations, allocations, firm and interruptible capacities, gas quality, congestion
  management, interruptions, tariffs, urgent market messages and reference data (points,
  operators, balancing zones, interconnections).
- **Key:** none.
- **Terms:** [ENTSOG Transparency Platform terms of use][entsog-terms] (revision 3, 24 May
  2018). They let you download, store and use the data, allow automated download through the
  API, and reserve every other right. Attribution, with the download date:
  "ENTSOG TP [DD-MM-YYYY] https://transparency.entsog.eu/".
- **Not settled:** the terms neither allow nor forbid republishing the data, and ENTSOG
  still hosts an [older 2016 revision][entsog-terms-2016] alongside them.
- **Fixtures:** the one file in `tests/fixtures/entsog/` is synthetic.

## GIE AGSI+ and ALSI (`gie_agsi`, `gie_alsi`)

- **Fetches:** from AGSI+ at `agsi.gie.eu`, 7 datasets: gas storage by country, company and
  facility, storage reports, the operator and facility listing, news and unavailability
  notices. From ALSI at `alsi.gie.eu`, 1 dataset: LNG terminal inventory and send-out.
- **Key:** `GIE_API_KEY`, free, sent as the `x-key` header. Register under API on
  [agsi.gie.eu][gie-agsi] or [alsi.gie.eu][gie-alsi]; one key works for both.
- **Terms:** the data-use rules on the [AGSI][gie-agsi] and [ALSI][gie-alsi] API pages say
  the data can be used or repackaged freely, as long as GIE is clearly named as the source.
  The minimum is a mention of GIE, AGSI or ALSI, and GIE may disable a key whose user gives
  no credit. The [API manual][gie-manual] (v013, 6 March 2025) adds no other rules.
- **Not settled:** GIE's [general website terms][gie-site-terms] forbid republishing
  website content, but they defer to the platforms' own conditions for AGSI+ and ALSI.
- **Fixtures:** 7 of the 8 files in `tests/fixtures/gie/` are synthetic (demo names, round
  values). `agsi_unavailability_response.json` looks real: real facilities, EIC codes and
  vendor notice text in the live record shape. Its origin isn't recorded. **Sharing:**
  allowed by the AGSI rule above, with GIE named as the source.

## NESO Carbon Intensity API (`neso`)

- **Fetches:** 33 datasets from `api.carbonintensity.org.uk`: national and regional carbon
  intensity (actual and forecast), generation mix, intensity factors and statistics.
- **Key:** none.
- **Terms:** the [Carbon Intensity site][neso-ci-home] says the API is licensed under CC BY
  4.0, which allows sharing with credit and a link to the licence. The [terms of
  use][neso-ci-terms], owned by NESO, ban selling or sublicensing access to the API and
  implying NESO's endorsement. They give no attribution wording.
- **Not settled:** the terms of use also reserve NESO's rights in the API's output, without
  saying how that fits the CC BY 4.0 label.
- **Fixtures:** `carbon_intensity_response.json` is synthetic. The two
  `regional_intensity_fw24h_*.json` files are trimmed live captures from 8 May 2026.
  **Sharing:** unclear, because of the clash above.

## NESO Data Portal (`neso_data_portal`)

- **Fetches:** the CKAN API at `api.neso.energy`. gridflow's registry
  (`src/gridflow/connectors/neso_data_portal/registry/`) lists 131 packages and 326 datasets:
  demand, generation, balancing costs, ancillary services, interconnectors, future energy
  scenarios, tariffs, system frequency and more. gridflow paces requests at one a second.
- **Key:** none.
- **Terms:** the [National Energy SO Open Data Licence v1.0][neso-dp-terms], based on the
  Open Government Licence v3.0. It allows copying, publishing, distributing, adapting and
  commercial use. Attribution: "Supported by National Energy SO Open Data". The licence
  is granted dataset by dataset; on 1 November 2026 the portal's CKAN API gave all 131
  packages the licence id `ESO`, which [its licence list][neso-dp-licences] maps to this
  licence.
- **Not settled:** the licence excludes third-party rights NESO can't license. The fixture
  `ic2/nordpool.csv` holds day-ahead prices that the
  [package page][neso-dp-nordpool] says Nord Pool provides. Two packages whose notes cite a
  third-party origin are held back from gridflow's tables, and no fixture comes from them.
- **Fixtures:** `tests/fixtures/neso_data_portal/` holds 501 files: 458 data files,
  28 `PROVENANCE.md` files and 15 git attribute files. Most data files are byte-for-byte
  cuts of real responses (CSV, Excel and ZIP), most of them captured on 8 October 2026, or
  NESO's own CKAN metadata. The `PROVENANCE.md` files and the test docstrings record each
  cut. A few files are not captures: golden copies of gridflow's own output (the largest
  file, `h/base_pin.json` at 2.2 MB, plus `gen2h/reconcile_base_pin.json` and
  `as5c/container_pin.json`) and hand-built files such as the two
  `embedded_forecast_dst_*.csv`. **Sharing:** allowed under the licence, with the
  attribution above.

## Open-Meteo (`open_meteo`)

- **Fetches:** 6 datasets of hourly weather from the forecast API (`api.open-meteo.com`) and
  the historical archive (`archive-api.open-meteo.com`), for 7 UK population centres (demand),
  12 GB wind sites and 6 GB solar sites.
- **Key:** none.
- **Terms:** the [terms][om-terms] limit the free API to non-commercial use, under 10,000
  calls a day, 5,000 an hour and 600 a minute. Commercial users need a paid plan, which
  gridflow doesn't support yet. The [licence page][om-licence] puts the data under CC BY 4.0
  and asks for a link next to wherever the data is shown: "Weather data by Open-Meteo.com".
- **Not settled:** the licence page lists the weather services behind the data (ECMWF,
  Copernicus, the UK Met Office and others) under their own licences; those haven't been
  checked here.
- **Fixtures:** the one file in `tests/fixtures/openmeteo/` is synthetic.

## Attribution for the real responses in this repository

The real responses under `tests/fixtures/` are included under these notices:

- NESO Data Portal: Supported by National Energy SO Open Data.
- NESO Carbon Intensity API: data from NESO's Carbon Intensity API, licensed under
  [CC BY 4.0](https://creativecommons.org/licenses/by/4.0/); trimmed to a few periods.
- ENTSO-E: source: ENTSO-E Transparency Platform; cut to three bid series.
- GIE: source: GIE AGSI+.

## Open questions for the maintainer

1. NESO Data Portal: keep `ic2/nordpool.csv`, or replace it with a synthetic file?
2. NESO Carbon Intensity: keep the two regional captures under CC BY 4.0?
3. ENTSO-E: is the balancing-bid dataset on the reuse list? If not, replace the German
   capture with a synthetic file.
4. GIE: confirm the AGSI rule covers `agsi_unavailability_response.json`, or replace it.
5. Elexon: confirm the licence covers data read through the Insights API.

[elexon-terms]: https://www.elexon.co.uk/data/balancing-mechanism-reporting-agent/copyright-licence-bmrs-data/
[entsoe-tp]: https://transparency.entsoe.eu/
[entsoe-token]: https://transparencyplatform.zendesk.com/hc/en-us/articles/12845911031188-How-to-get-security-token
[entsoe-terms]: https://eepublicdownloads.entsoe.eu/clean-documents/mc-documents/transparency-platform/ENTSOE_Transparency_Terms_Conditions.pdf
[entsoe-reuse-list]: https://transparencyplatform.zendesk.com/hc/en-us/articles/40921911218961-Legal-Terms-and-Conditions
[entsog-terms]: https://transparency.entsog.eu/pdf/TRA0394_20161115_ENTSOG_TP_TC_of_Use_Rev_2_KNS_LG_KNS.pdf
[entsog-terms-2016]: https://transparency.entsog.eu/pdf/Terms_and_Conditions_of_Use.pdf
[gie-agsi]: https://agsi.gie.eu/account
[gie-alsi]: https://alsi.gie.eu/account
[gie-manual]: https://www.gie.eu/transparency-platform/GIE_API_documentation_v013.pdf
[gie-site-terms]: https://www.gie.eu/privacy-policy
[neso-ci-home]: https://carbonintensity.org.uk/
[neso-ci-terms]: https://github.com/carbon-intensity/terms
[neso-dp-terms]: https://www.neso.energy/data-portal/neso-open-licence
[neso-dp-licences]: https://api.neso.energy/api/3/action/license_list
[neso-dp-nordpool]: https://www.neso.energy/data-portal/day-ahead-power-exchange-prices-nordpool
[om-terms]: https://open-meteo.com/en/terms
[om-licence]: https://open-meteo.com/en/licence
