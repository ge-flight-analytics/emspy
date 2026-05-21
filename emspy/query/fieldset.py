from urllib.error import HTTPError

import pandas as pd

from .asset import Asset


# Response parsing helpers ---------------------------------------------------

def _first(d, *keys):
    # Tolerant key lookup: returns the first non-None value among d[k1], d[k2], ...
    # Mirrors the R %||% operator used in Rems2.
    if not isinstance(d, dict):
        return None
    for k in keys:
        if k in d and d[k] is not None:
            return d[k]
    return None


def _parse_fieldset_group_entry(d):
    return {
        'id': _first(d, 'id', 'groupId', 'group_id'),
        'name': _first(d, 'name'),
        'description': _first(d, 'description'),
    }


def _parse_fieldset_entry(d):
    # Slim summary form that appears in group listings.
    return {
        'id': _first(d, 'id', 'name'),
        'name': _first(d, 'name'),
        'description': _first(d, 'description'),
    }


def _parse_fieldset_fields(fields):
    # Direct port of Rems2's parse_fieldset_fields. Tolerates schemaItems with
    # moniker/description, or fields with id/name, or bare-string entries.
    cols = {'id': [], 'name': [], 'type': []}
    if not fields:
        return pd.DataFrame(cols, columns=['id', 'name', 'type'])

    for f in fields:
        if isinstance(f, str):
            cols['id'].append(f)
            cols['name'].append(None)
            cols['type'].append(None)
            continue
        if not isinstance(f, dict):
            continue
        cols['id'].append(_first(f, 'moniker', 'id', 'fieldId'))
        cols['name'].append(_first(f, 'description', 'name'))
        cols['type'].append(_first(f, 'type', 'dataType', 'data_type'))

    return pd.DataFrame(cols, columns=['id', 'name', 'type'])


# Fieldset class -------------------------------------------------------------

class Fieldset(Asset):
    """
    Server-curated bundles of fields exposed by the EMS fieldset-groups API.

    Fieldsets are metadata only: the /query endpoint does not accept a
    fieldset reference, so a fieldset is "applied" client-side by expanding
    its fields into a FltQuery's select list. See FltQuery.select_fieldset.

    Parameters
    ----------
    conn : emspy.connection.Connection
        Connection object.
    ems_id : int
        EMS system ID.
    """

    def __init__(self, conn, ems_id):
        Asset.__init__(self, conn, "Fieldset")
        self._ems_id = ems_id
        # Session-level caches; mirrors Rems2's pkg_env$fieldset_cache structure.
        self._root_cache = None                  # DataFrame of groups, or None
        self._group_cache = {}                   # group_id -> DataFrame
        self._fieldset_cache = {}                # (group_id, name) -> dict

    # Public API -------------------------------------------------------------

    def get_groups(self, refresh=False):
        """
        List the top-level fieldset groups available on the EMS system.

        Returns a DataFrame with columns: id, name, description. Cached
        results are returned as copies so callers cannot mutate the cache.
        """
        if not refresh and self._root_cache is not None:
            return self._root_cache.copy()

        _, result = self._conn.request(
            uri_keys=('fieldset', 'fieldset_groups'),
            uri_args=self._ems_id,
        )
        # Tolerate either bare-array or {'groups': [...]} envelope.
        group_list = result.get('groups') if isinstance(result, dict) else result
        if group_list is None:
            group_list = result if isinstance(result, list) else []

        if not group_list:
            df = pd.DataFrame({'id': [], 'name': [], 'description': []},
                              columns=['id', 'name', 'description'])
        else:
            df = pd.DataFrame.from_records(
                [_parse_fieldset_group_entry(x) for x in group_list]
            )
            df = df[['id', 'name', 'description']]

        self._root_cache = df
        return df.copy()

    def get_group(self, group_id, refresh=False):
        """
        Get a single fieldset group's contents.

        Returns a DataFrame with columns: type (group|fieldset), id, name,
        description. The id for a fieldset row is its name (which is what
        the API expects in subsequent calls).
        """
        if not isinstance(group_id, str) or not group_id:
            raise ValueError("group_id must be a non-empty string.")

        if not refresh and group_id in self._group_cache:
            return self._group_cache[group_id].copy()

        _, result = self._conn.request(
            uri_keys=('fieldset', 'fieldset_group'),
            uri_args=(self._ems_id, group_id),
        )

        subgroups = result.get('groups') or []
        fieldsets = (
            result.get('fieldSets')
            or result.get('fieldsets')
            or result.get('field_sets')
            or []
        )

        rows = []
        for g in subgroups:
            entry = _parse_fieldset_group_entry(g)
            rows.append({
                'type': 'group',
                'id': entry['id'],
                'name': entry['name'],
                'description': entry['description'],
            })
        for f in fieldsets:
            entry = _parse_fieldset_entry(f)
            rows.append({
                'type': 'fieldset',
                'id': entry['id'],
                'name': entry['name'],
                'description': entry['description'],
            })

        if rows:
            df = pd.DataFrame.from_records(rows)
            df = df[['type', 'id', 'name', 'description']]
        else:
            df = pd.DataFrame(
                {'type': [], 'id': [], 'name': [], 'description': []},
                columns=['type', 'id', 'name', 'description'],
            )

        self._group_cache[group_id] = df
        return df.copy()

    def get_fieldset(self, group_id, fieldset_name, refresh=False):
        """
        Fetch a single fieldset.

        Returns a dict:
            {
                'name': str,
                'group_id': str,
                'ems_id': int,
                'fields': DataFrame with columns id, name, type
            }
        """
        if not isinstance(group_id, str) or not group_id:
            raise ValueError("group_id must be a non-empty string.")
        if not isinstance(fieldset_name, str) or not fieldset_name:
            raise ValueError("fieldset_name must be a non-empty string.")

        key = (group_id, fieldset_name)
        if not refresh and key in self._fieldset_cache:
            return self._copy_fieldset(self._fieldset_cache[key])

        _, result = self._conn.request(
            uri_keys=('fieldset', 'fieldset'),
            uri_args=(self._ems_id, group_id, fieldset_name),
        )

        raw_fields = result.get('schemaItems')
        if raw_fields is None:
            raw_fields = result.get('fields')
        if raw_fields is None:
            print("-- Warning: fieldset '%s' response has no recognised "
                  "field list (expected 'schemaItems' or 'fields')."
                  % fieldset_name)
            raw_fields = []

        fields_df = _parse_fieldset_fields(raw_fields)

        fs = {
            'name': result.get('name') or fieldset_name,
            'group_id': group_id,
            'ems_id': self._ems_id,
            'fields': fields_df,
        }
        self._fieldset_cache[key] = fs
        return self._copy_fieldset(fs)

    @staticmethod
    def _copy_fieldset(fs):
        # Shallow-copy the dict with a fresh DataFrame so mutations to the
        # returned value (e.g. ``fs['fields'].drop(...)``) don't bleed
        # into the cache.
        return {
            'name': fs['name'],
            'group_id': fs['group_id'],
            'ems_id': fs['ems_id'],
            'fields': fs['fields'].copy(),
        }

    def find(self, fieldset_name, max_depth=6):
        """
        Depth-first search across all visible groups for a fieldset by name.

        Returns a list of {'group_id': str, 'path': [str, ...]} dicts, one per
        group whose immediate contents contain a fieldset matching
        fieldset_name. Permission-gated subgroups are silently skipped.
        """
        if not isinstance(fieldset_name, str) or not fieldset_name:
            raise ValueError("fieldset_name must be a non-empty string.")

        hits = []
        groups = self.get_groups()
        if len(groups) == 0:
            return hits

        def visit(gid, path, depth):
            if depth > max_depth:
                return
            try:
                contents = self.get_group(gid)
            except Exception as e:
                # Permission-gated or otherwise inaccessible: skip but tell
                # the user, so the HTTP error noise printed by the connection
                # layer doesn't look like a failure of find() itself.
                group_label = path[-1] if path else gid
                if isinstance(e, HTTPError):
                    reason = "HTTP %d %s" % (e.code, e.reason)
                else:
                    reason = str(e) or type(e).__name__
                print(
                    "-- Skipping fieldset group '%s' (id=%s) during tree "
                    "walk: %s. Continuing search."
                    % (group_label, gid, reason)
                )
                return
            if contents is None or len(contents) == 0:
                return

            target = fieldset_name.lower()
            fs_rows = contents[
                (contents['type'] == 'fieldset')
                & (contents['name'].str.lower() == target)
            ]
            for _, fs_row in fs_rows.iterrows():
                hits.append({
                    'group_id': gid,
                    'name': fs_row['name'],
                    'path': list(path),
                })

            sub_rows = contents[contents['type'] == 'group']
            for _, row in sub_rows.iterrows():
                visit(row['id'], path + [row['name']], depth + 1)

        for _, row in groups.iterrows():
            visit(row['id'], [row['name']], depth=1)

        return hits

    def clear_cache(self):
        """Empty the in-memory cache so subsequent calls re-fetch from the API."""
        self._root_cache = None
        self._group_cache = {}
        self._fieldset_cache = {}
