# History

## v1.13.0 (unreleased)

- Snapshots support an explicit authoritative scope for hard-delete detection
  (`hard_deletes_scope_column` / `hard_deletes_scope_values`). Only open records
  inside the declared scope (e.g. the tenants actually collected in this run) are
  compared for missing rows and produce delete history; records outside the scope
  keep their current and historical versions unchanged, including when the source
  is empty. A source row carrying an undeclared scope value fails the run before
  the snapshot table is modified. Works with both `hard_deletes = 'invalidate'`
  and `hard_deletes = 'new_record'`, including re-appearing records.

## v0.1.0 (2020-06-02)

- First release on PyPI.

## v0.2.0 (2020-06-02)

- Added full macro implementation. Thanks Fabrice Etanchaud

## v0.4.3 (2021-06-28)

- Last stable release of [dbt-oracle](https://github.com/techindicium/dbt-oracle) by Techindicium

## v1.0.0 (2022-05-04)

- First release of vendor-built adapter by Oracle. 
- This version supports Python 3.6, 3.7, 3.8 and 3.9
