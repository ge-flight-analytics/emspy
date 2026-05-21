# Changelog

## 0.7.0

### New features

- `FltQuery.select_fieldset(fieldset, group=None)` expands a server-curated
  fieldset into the query's select list in one call. Accepts either a
  pre-fetched fieldset dict or a name; without `group`, walks the
  fieldset-group tree depth-first to find a unique match (name matching is
  case-insensitive). Fields are expanded client-side into individual
  `{fieldId, aggregate='none'}` entries - the `/query` endpoint never sees
  the fieldset reference. Aggregation is not exposed at the fieldset level;
  use `select()` / `select_id()` for per-field aggregation.
- New `Fieldset` class (in `emspy/query/fieldset.py`) wraps the GE EMS
  `/fieldset-groups` endpoint family. Exposes `get_groups()`,
  `get_group(group_id)`, `get_fieldset(group_id, name)`, `find(name)` and
  `clear_cache()`. Tolerates the observed response-shape variations
  (`schemaItems` with `moniker`/`description` vs `fields` with `id`/`name`,
  bare-string entries, bare-array vs envelope group listings).
- `select_fieldset()` verifies that the fieldset's embedded
  `[ems-...][entity-type][...]` token agrees with the query's currently
  selected database; aborts with a clear message on mismatch or on a
  fieldset that mixes fields from multiple databases. Skipped silently when
  no database has been selected yet, or when field IDs lack an extractable
  entity-type token.
- `FltQuery.select_id(*field_ids, aggregate='none')` selects fields by their
  id (moniker) directly, bypassing name-based metadata tree search. Faster
  and unambiguous when the field id is already known. Backed by a new
  `Flight.resolve_id()` that checks the local fieldtree cache first and
  falls back to the EMS field API, caching the result.
- `deselect()` no longer requires the targeted field to be present in the
  local fieldtree. It now matches the supplied keyword directly against the
  current selection - first by exact field id, then by case-insensitive
  substring against the field name as it appears in the select list. This
  fixes a gap where fields added via `select_fieldset()` or via the API
  fallback path of `select_id()` could not be removed. Raises `ValueError`
  with the current selection listed if nothing matches, and prints a line
  naming the fields that were removed.

  Default behaviour matches the pre-0.7 `search_fields(unique=True)`
  semantics: when a substring matches more than one selected field, only
  the shortest-named match is removed. Pass `all_matches=True` to remove
  every matching field in a single call.

### Bug fixes

- pandas 3.0 compatibility:
  - Replace positional `df.iloc[:, i]` assignment with `df.isetitem(i, ...)`
    for dtype conversions in `FltQuery.__to_dataframe()` (fixes `TypeError`
    with PyArrow `StringDtype`).
  - Widen the dtype-conversion `except` clause to catch `TypeError` in
    addition to `ValueError`.
  - Replace `inplace=True` with reassignment for Copy-on-Write compatibility
    (`profile.py` `set_index`, `flight.py` `reset_index`).
  - Fix empty-DataFrame `concat` pattern in `analyticset.py` using
    `reindex`.
  - Fix `.astype(int)` on a string column in `profile.py` using
    `pd.to_numeric`.
  - Fix invalid escape sequences in `analyticset.py` and
    `test_analyticset.py`.
  - Fix duplicate-column `TypeError` in `FltQuery.__to_dataframe` by
    reading positionally via `df.iloc[:, i]` and writing via
    `df.isetitem(i, ...)`.
- Fix `np.int64` parameters breaking `sqlite3` queries under numpy 2.x.
  Convert numpy scalar types to native Python types via `.item()` before
  passing to `sqlite3` in `localdata.py` `get_data()` and `delete_data()`.
- Fix SQL injection vulnerability in `query.set_database()` when database
  names contain single quotes (e.g. `"GPWS: Don't Sink"`). `localdata.py`
  `get_data()` and `delete_data()` now support parameterized queries via a
  `(where_clause, params)` tuple; callers in `flight.py` and `analytic.py`
  switched over. Backward compatible with the existing string-based API.
- Fix `tsquery` `PerformanceWarning` for queries with many parameters by
  building the result as a dict before constructing the DataFrame instead
  of appending columns one by one. Ensure the `offsets` element exists in
  the `tsQuery` run result.
- Drop `path` and `displayPath` from `tsquery` select to fix indexing.
- Several follow-up `pd.concat()` fixes across `FltQuery` query/mode paths.

### Packaging / requirements

- Drop Python 2.7 support from `setup.py` classifiers.
- Add `pandas>=1.5` minimum-version constraint.
- Add `smoke_test_*.py` to `.gitignore`.
- Remove an auto-generated `pyproject.toml` that declared
  `requires-python>=3.13` and broke CI.

### Documentation

- README: new "Selecting a fieldset" section showing the three call forms
  for `select_fieldset()`.
- README: new section documenting `select_id()`.
- README: misc updates from 0.6.x.

## 0.6.0

Baseline for this changelog.
