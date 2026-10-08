---
source: "neso_data_portal"
package: "transmission-entry-capacity-tec-register"
dataset_key: "tec_register"
vendor: "NESO Open Data Portal"
skeleton: true
layer_coverage: "bronze, silver"
eligibility: "eligible"
---

# Transmission Entry Capacity (TEC) register

> TODO: overview prose (docs wave, gridflow-dataset-spec).

- Publisher group: Connection registers
- Registry group: connection-registers · archetype: REG

## Files by disposition

### SILVER

| Resource | Format | Capture | Id |
|---|---|---|---|
| TEC Register | CSV | upload | `17becbab-e3e8-473f-b303-3806f43a6a10` |

## Families

### `tec_register`

- Kind: tabular · archetype: REG · refresh: daily · empty allowed: yes

Schema, epoch 1:

| Vendor column | Silver column | Dtype | Format | Nullable | Zone | Vendor unit |
|---|---|---|---|---|---|---|
| Project Name | `project_name` | string | — | yes | — | — |
| Customer Name | `customer_name` | string | — | yes | — | — |
| Connection Site | `connection_site` | string | — | yes | — | — |
| Stage | `stage` | float64 | — | yes | — | number |
| MW Connected | `mw_connected` | float64 | — | yes | — | MW |
| MW Increase / Decrease | `mw_increase_decrease` | float64 | — | yes | — | MW |
| Cumulative Total Capacity (MW) | `cumulative_total_capacity_mw` | float64 | — | yes | — | MW |
| MW Effective From | `mw_effective_from` | date | `%d/%m/%Y` | yes | — | yyyy-mm-dd |
| Project Status | `project_status` | string | — | yes | — | — |
| Agreement Type | `agreement_type` | string | — | yes | — | — |
| HOST TO | `host_to` | string | — | yes | — | — |
| Plant Type | `plant_type` | string | — | yes | — | — |
| Project ID | `project_id` | string | — | yes | — | — |
| Project Number | `project_number` | string | — | yes | — | — |
| Gate | `gate` | float64 | — | yes | — | number |

- Entity key: `project_name`, `customer_name`, `connection_site`, `stage`, `mw_connected`, `mw_increase_decrease`, `cumulative_total_capacity_mw`, `mw_effective_from`, `project_status`, `agreement_type`, `host_to`, `plant_type`, `project_id`, `project_number`, `gate`
- Latest: `whole_capture`
- Temporal recipe: none: `timestamp_utc` is the capture time

| Clock | Meaning |
|---|---|
| `published_at` | CKAN `last_modified` of the captured file (ADR-030) |
| `available_at` | = `published_at` |

## Cadence

- Registry refresh: daily
- Vendor metadata (snapshot extras, verbatim):
  - Update Frequency: Twice weekly

## Licence and attribution

- Licence: NESO Open Data Licence
- Attribution: Supported by National Energy SO Open Data

## Holds

- none
