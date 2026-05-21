from __future__ import absolute_import
from __future__ import print_function

import json
import re
import sys
from builtins import str
from builtins import zip
from collections import OrderedDict

import pandas as pd
from future.utils import string_types

from emspy.query import *
from .query import Query
from .fieldset import Fieldset


# Field IDs embed the source entity-type as a `[ems-...][entity-type][...]`
# token, which is also the form returned by /database-groups and used as
# Flight._db_id. This helper extracts that token so a fieldset's fields can
# be compared against the query's selected database.
_ENTITY_TYPE_RE = re.compile(r"\[ems-[a-z0-9]+\]\[entity-type\]\[[^\]]+\]")


def _extract_field_entity_type(field_id):
    if not isinstance(field_id, string_types):
        return None
    m = _ENTITY_TYPE_RE.search(field_id)
    return m.group(0) if m else None


class FltQuery(Query):
    """
    Flight query class
    """
    def __init__(self, conn, ems_name=None, data_file=LocalData.default_data_file):
        """
        Flight query initialization

        Parameters
        ----------
        conn: emspy.connection.Connection
            connection object
        ems_name: str
            EMS system name
        data_file: str
            path to database file
        """
        Query.__init__(self, conn, ems_name)
        self._init_assets(data_file)
        self.reset()

    def _init_assets(self, data_file):
        # Query._init_assets(self)
        self.__flight = Flight(self._conn, self._ems_id, data_file)
        self.__fieldset = Fieldset(self._conn, self._ems_id)

    def set_database(self, name):
        """
        Set database for querying

        Parameters
        ----------
        name: str
            database name

        Returns
        -------
        None
        """
        self.__flight.set_database(name)

    def get_database(self):
        """
        Get selected database

        Returns
        -------
        dict
            dictionary for the selected database
        """
        return self.__flight.get_database()

    def reset(self):
        """
        Resets the API query to send. This should be called before creating a new query.

        Returns
        -------
        None
        """
        self.__columns = []
        self.__queryset = {
            "select": [],
            "groupBy": [],
            "orderBy": [],
            "distinct": True,
            "format": "none"
        }

    def select(self, *args, **kwargs):
        """
        Functionally equivalent to SQL's select statement

        Parameters
        ----------
        args:
            fields to query
        kwargs:
            keyword arguments

        Keyword arguments
        -----------------
        aggregate: str
            aggregation to apply, one of:
            ['none', 'avg', 'count', 'max', 'min', 'stdev', 'sum', 'var']

        Returns
        -------
        None

        Examples
        --------
        Following is the example of select method to query three fields and one more with
        aggregation function applied. The values are appended until the whole query is
        reset.

        >>> query.select("customer id", "takeoff valid", "takeoff airport iata code")
        >>> query.select("p301: fuel burned by all engines during cruise", aggregate="avg")
        """
        aggs = ['none', 'avg', 'count', 'max', 'min', 'stdev', 'sum', 'var']
        aggregate = kwargs.get('aggregate', 'none')
        if aggregate not in aggs:
            sys.exit("Wrong aggregation selected. Use one of %s." % aggs)
        fields = self.__flight.search_fields(*args, **kwargs)

        if not isinstance(fields, list):
            fields = [fields]

        for field in fields:
            d = {
                'fieldId': field['id'],
                'aggregate': aggregate
            }
            self.__queryset['select'].append(d)
            self.__columns.append(field)

    def select_id(self, *args, **kwargs):
        """
        Select fields by their id (moniker) directly, bypassing name-based
        metadata tree search. This is faster and avoids ambiguity when the
        field id is already known.

        Each id that is not already in the local fieldtree cache triggers
        one ``/ems-systems/<ems_id>/databases/<db_id>/fields/<field_id>``
        API call via ``Flight.resolve_id`` (the result is cached for the
        rest of the session). For bulk selection of many uncached ids,
        consider calling ``update_fieldtree(...)`` first to prime the
        cache in a single tree walk.

        Parameters
        ----------
        args:
            Field id strings identifying fields to query
        kwargs:
            keyword arguments

        Keyword arguments
        -----------------
        aggregate: str
            aggregation to apply, one of:
            ['none', 'avg', 'count', 'max', 'min', 'stdev', 'sum', 'var']

        Returns
        -------
        None

        Examples
        --------
        >>> query.select_id(
        ...     "[-hub-][field][[[ems-core][entity-type][foqa-flights]]"
        ...     "[[ems-core][base-field][flight.uid]]]"
        ... )
        >>> query.select_id("some-field-id", aggregate="avg")
        """
        aggs = ['none', 'avg', 'count', 'max', 'min', 'stdev', 'sum', 'var']
        aggregate = kwargs.get('aggregate', 'none')
        if aggregate not in aggs:
            sys.exit("Wrong aggregation selected. Use one of %s." % aggs)

        for field_id in args:
            field = self.__flight.resolve_id(field_id)
            d = {
                'fieldId': field['id'],
                'aggregate': aggregate
            }
            self.__queryset['select'].append(d)
            self.__columns.append(field)

    def select_fieldset(self, fieldset, group=None):
        """
        Expand a server-curated fieldset into the query's select list.

        Fields are expanded client-side: every field in the fieldset is
        appended as an individual {fieldId, aggregate} entry with
        aggregate='none'. The /query endpoint never sees the fieldset
        reference.

        Aggregation is intentionally not exposed here: applying a single
        aggregation uniformly across every field in a curated fieldset is
        rarely sensible, and some aggregations (e.g. stdev on datetime
        fields) are not processed correctly by the API. To aggregate, add
        the relevant fields individually via select() or select_id().

        Parameters
        ----------
        fieldset: str or dict
            Either a fieldset name (str) or a pre-fetched fieldset dict
            returned by Fieldset.get_fieldset. If a name is given without
            `group`, the fieldset-group tree is walked to find a unique
            match (name matching is case-insensitive).
        group: str, optional
            Fieldset-group ID. Skips the tree walk when `fieldset` is a name.

        Examples
        --------
        >>> query.select_fieldset("Standard Flight Metrics")
        >>> query.select_fieldset("Standard Flight Metrics", group="<group-id>")
        >>> fs = Fieldset(conn, ems_id).get_fieldset("<group-id>", "Standard Flight Metrics")
        >>> query.select_fieldset(fs)
        """
        fs = self._resolve_fieldset_arg(fieldset, group)
        fields_df = fs['fields']

        if len(fields_df) == 0:
            print("-- Warning: fieldset '%s' contains no fields; nothing added."
                  % fs.get('name', '<unknown>'))
            return

        self._verify_fieldset_database_match(fs)

        added = 0
        for _, row in fields_df.iterrows():
            field_id = row.get('id')
            if not field_id or (isinstance(field_id, float) and pd.isna(field_id)):
                print("-- Skipping field '%s' in fieldset '%s': missing field ID."
                      % (row.get('name'), fs.get('name', '<unknown>')))
                continue
            self.__queryset['select'].append({
                'fieldId': field_id,
                'aggregate': 'none',
            })
            self.__columns.append({
                'id': field_id,
                'name': row.get('name') or field_id,
                'type': row.get('type'),
            })
            added += 1

        print("-- Added %d field%s from fieldset '%s'."
              % (added, 's' if added != 1 else '', fs.get('name', '<unknown>')))

    def _resolve_fieldset_arg(self, fieldset, group):
        # Pre-fetched fieldset dict.
        if isinstance(fieldset, dict) and isinstance(fieldset.get('fields'), pd.DataFrame):
            return fieldset

        if not isinstance(fieldset, string_types):
            raise TypeError(
                "fieldset must be a fieldset name (str) or a fieldset dict "
                "returned by Fieldset.get_fieldset()."
            )

        if group is not None:
            return self.__fieldset.get_fieldset(group, fieldset)

        # No group supplied: walk the tree for a unique match.
        hits = self.__fieldset.find(fieldset)
        if len(hits) == 0:
            raise ValueError(
                "Fieldset '%s' not found in any visible fieldset group. "
                "List groups with Fieldset.get_groups() and contents with "
                "Fieldset.get_group(group_id)." % fieldset
            )
        if len(hits) > 1:
            paths = [' > '.join(h['path']) for h in hits]
            raise ValueError(
                "Fieldset name '%s' is ambiguous - found in multiple groups:\n  %s\n"
                "Pass `group` explicitly to disambiguate."
                % (fieldset, '\n  '.join(paths))
            )
        return self.__fieldset.get_fieldset(hits[0]['group_id'], hits[0]['name'])

    def _verify_fieldset_database_match(self, fs):
        # Skipped silently when no database has been selected yet (the user can
        # call set_database() later) or when no field IDs carry an extractable
        # entity-type token (e.g. mock/test field IDs).
        query_db = self.__flight.get_db_id()
        if query_db is None:
            return

        entity_types = []
        for fid in fs['fields']['id'].tolist():
            et = _extract_field_entity_type(fid)
            if et:
                entity_types.append(et)
        if not entity_types:
            return

        unique_types = sorted(set(entity_types))
        if len(unique_types) > 1:
            raise ValueError(
                "Fieldset '%s' mixes fields from multiple databases; cannot "
                "apply as a single select. Distinct source databases:\n  %s"
                % (fs.get('name', '<unknown>'), '\n  '.join(unique_types))
            )

        expected_db = unique_types[0]
        # Tolerate stray whitespace on user-supplied database IDs (a common
        # copy/paste hazard). Case and bracket structure remain significant:
        # entity tokens embed case-sensitive UUID / profile hashes.
        if query_db.strip() != expected_db.strip():
            raise ValueError(
                "Fieldset '%s' targets a different database than this query.\n"
                "  Query database:    %s\n"
                "  Fieldset database: %s\n"
                "Rebuild the query with set_database() on the fieldset's "
                "database, or pick a fieldset for the current database."
                % (fs.get('name', '<unknown>'), query_db, expected_db)
            )

    def deselect(self, *args, **kwargs):
        """
        Remove fields from the current selection.

        For each argument, match against the currently-selected fields:
        first by exact field id, then by case-insensitive substring against
        the selected field name. No fieldtree lookup is involved, so this
        works uniformly for fields added via ``select()``, ``select_id()``,
        or ``select_fieldset()``.

        When a substring matches more than one selected field, by default
        only the single shortest-named match is removed (preserving the
        pre-0.7 ``unique=True`` ``search_fields`` semantics). Pass
        ``all_matches=True`` to remove every matching field.

        Raises ``ValueError`` if no field matches.

        Parameters
        ----------
        args:
            Field ids (exact) or substrings of selected field names.

        Keyword arguments
        -----------------
        all_matches: bool
            If True, remove every selected field whose name contains the
            substring. Default False (remove only the shortest-named match).

        Returns
        -------
        None
        """
        all_matches = kwargs.pop('all_matches', False)
        if kwargs:
            raise TypeError(
                "deselect() got unexpected keyword arguments: %s"
                % list(kwargs.keys())
            )

        for arg in args:
            # Pass 1: exact id match (unambiguous).
            matched = [c for c in self.__columns if c.get('id') == arg]

            # Pass 2: case-insensitive substring match on the column name.
            if not matched and isinstance(arg, string_types):
                arg_lower = arg.lower()
                matched = [
                    c for c in self.__columns
                    if isinstance(c.get('name'), string_types)
                    and arg_lower in c['name'].lower()
                ]
                # Default to legacy single-removal: keep only the
                # shortest-named match unless caller opted in to all.
                if len(matched) > 1 and not all_matches:
                    matched = [min(matched, key=lambda c: len(c['name']))]

            if not matched:
                preview = [{'id': c.get('id'), 'name': c.get('name')}
                           for c in self.__columns[:10]]
                extra = len(self.__columns) - len(preview)
                tail = (" ...and %d more" % extra) if extra > 0 else ""
                raise ValueError(
                    "deselect(%r): no matching field in the current selection. "
                    "Pass either the exact field id or a substring of the "
                    "selected field name. Currently selected: %s%s"
                    % (arg, preview, tail)
                )

            ids_to_remove = {c['id'] for c in matched}
            self.__queryset['select'] = [
                d for d in self.__queryset['select']
                if d['fieldId'] not in ids_to_remove
            ]
            self.__columns = [c for c in self.__columns if c['id'] not in ids_to_remove]

            removed = [(c.get('name') or c.get('id'), c.get('id')) for c in matched]
            if len(removed) == 1:
                print("-- Deselected '%s' (%s)." % removed[0])
            else:
                print("-- Deselected %d fields: %s"
                      % (len(removed),
                         ", ".join("'%s' (%s)" % r for r in removed)))

    def group_by(self, *args):
        """
        Functionally equivalent to SQL's groupby

        Parameters
        ----------
        args:
            fields

        Returns
        -------
        None
        """
        for field in self.__flight.search_fields(*args):
            self.__queryset['groupBy'].append({'fieldId': field['id']})

    def order_by(self, field, order='asc'):
        """
        Functional equivalent of SQL's order by

        Parameters
        ----------
        field: str
            field
        order: str
            ordering order:
                'asc': ascending
                'desc': descending

        Returns
        -------
        None
        """
        if order not in ['asc', 'desc']:
            sys.exit("Ordering option must be one of %s." % ['asc', 'desc'])
        self.__queryset['orderBy'].append({
            'fieldId': self.__flight.search_fields(field)[0]['id'],
            'order': order,
            'aggregate': 'none'})

    def filter(self, expr, operator='and'):
        """
        Translate the give filtering conditions to json queries.

        Parameters
        ----------
        expr: str
            filtering expression
        operator: str
            specifies how to aggregate filters:
                and: all filter conditions must be met
                or: any filter condition must be met

        Returns
        -------
        None
        """
        if 'filter' not in self.__queryset:
            self.__queryset['filter'] = dict(operator=operator, args=[])
        expr_vec = self.__split_expr(expr)
        jsonobj = self.__translate_expr(expr_vec)
        self.__queryset['filter']['args'].append(jsonobj)

    def remove_filter(self, expr):
        """
        Removes a filter from the queryset

        Parameters
        ----------
        expr: str
            filtering expression

        Returns
        -------
        None
        """
        if 'filter' in self.__queryset:
            expr_vec = self.__split_expr(expr)
            jsonobj = self.__translate_expr(expr_vec)
            # Remove the filter from the arguments
            self.__queryset['filter']['args'].remove(jsonobj)
            # If no filters are left remove the filter key from the queryset
            if len(self.__queryset['filter']['args']) == 0:
                self.__queryset.remove('filter')

    @staticmethod
    def __split_expr(expr):
        # This function might need to be updated to be more robust. The split of a
        # given expression will not be correctly done if either field or field
        # value contains symbols identical to filtering operators.
        #
        # In fact the correction was already applied in Rems so refer to Rems'
        # split_expr function to check what was done there.
        import re

        any_valid_match = 0
        for pattern in ['[=!<>]=?'] + list(sp_ops.keys()):
            # The regular expression looks for either of these patterns in the filter:
            # 1. <variable> <operator> <value> (i.e. "'x' < '1'")
            # 2. <value> <operator> <variable> <operator> <value> (i.e. "'0' < 'x' < '1'")
            # Pattern 1 is the standard for filtering
            # Pattern 2 is only used for 'between' filtering
            if pattern not in ['is null', 'is not null']:
                match = re.search(
                    r"(.*)\s+(%s)\s+(.*)\s+(%s)\s+(.*)|(.*)\s+(%s)\s+(.*)".replace('%s', pattern),
                    expr
                )
            else:
                match = re.search(r"(.*)\s+(%s)$".replace('%s', pattern), expr)
            if match is not None:
                any_valid_match = len([group for group in match.groups() if group is not None])
                if any_valid_match:
                    break

        if match is None or not any_valid_match:
            sys.exit("Cannot find any valid conditional operator from the given string expression.")
        split = [group.strip() for group in match.groups() if group is not None]
        return split

    def __translate_expr(self, expr_vec):
        fld_loc = [False, False]
        fld_info = None
        fld_type = None
        val_info = []

        # If the expression vector contains 3 elements the filter is in the form
        # <variable> <operator> <value>
        # The operator is the second item of the list
        if len(expr_vec) in [2, 3]:
            op = expr_vec[1]
        # If the expression vector contains 5 elements the filter is in the form
        # <value> <operator> <variable> <operator> <value>
        # The operators are the second and fourth elements of the list
        elif len(expr_vec) == 5:
            op = [expr_vec[1], expr_vec[3]]

        # Get the list of operators in the expression
        op_vec = [op] if not isinstance(op, list) else op
        # Remove the operator[s] from the expression vector
        fld_val_vec = [s for s in expr_vec if s not in op_vec]
        for i, s in enumerate(fld_val_vec):
            x = eval(s)  # FIXME Remove eval
            # If the length of the expression vector is 3, the field is the first item in the list.
            # Otherwise, if the length of the expression vector is 5, the second item in the list
            # is the field.
            if i == 0 if len(expr_vec) in [2, 3] else i == 1:
                fld = self.__flight.search_fields(x)[0]
                if fld is not None:
                    fld_info = [{'type': 'field', 'value': fld['id']}]
                    fld_type = fld['type']
                    fld_loc[i] = True
                else:
                    raise ValueError("No field was found with the keyword %s. "
                                     "Please double-check if it is a right keyword." % x)
            else:
                if type(x) != list:
                    x = [x]
                val_info += [{'type': 'constant', 'value': v} for v in x]

        # This logic only applies to standard conditional operators
        if fld_loc[1] and len(expr_vec) == 3:
            if '<' in expr_vec[1]:
                op = expr_vec[1].replace('<', '>')
            else:
                op = expr_vec[1].replace('>', '<')

        arg_list = fld_info + val_info

        if fld_type == "boolean":
            fltr = _boolean_filter(op, arg_list)
        elif fld_type == "discrete":
            fltr = _discrete_filter(op, arg_list, self.__flight)
        elif fld_type == "number":
            fltr = _number_filter(op, arg_list)
        elif fld_type == "string":
            fltr = _string_filter(op, arg_list)
        elif fld_type == "dateTime":
            fltr = _datetime_filter(op, arg_list)
        else:
            raise ValueError("%s has an unknown field data type %s." % (fld[0], fld_type))
        return fltr

    def distinct(self, x=True):
        """
        Functionally equivalent to SQL's distinct

        Parameters
        ----------
        x: bool
            sets distinct flag if True

        Returns
        -------
        None
        """
        self.__queryset['distinct'] = x

    def get_top(self, n):
        """
        Functionally equivalent to SQL's limit or top
        Parameters
        ----------
        n: int
            number of records to limit the query to

        Returns
        -------
        None
        """
        self.__queryset['top'] = n

    # def readable_output(self, x=False):
    #     if x:
    #         y = "display"
    #     else:
    #         y = "none"
    #     self.__queryset['format'] = y

    def from_json_string(self, json_str):
        """
        Load the JSON string for this query.

        Parameters
        ----------
        json_str: str
            Queryset as a JSON str.
        """

        if type(json_str) is not str:
            raise TypeError('json_str should be a str. Found: {0}'.format(type(json_str)))

        self.__queryset = json.loads(json_str)

    def in_json(self):
        """
        Dump the queryset as JSON

        Returns
        -------
        str
            queryset as JSON string
        """
        return json.dumps(self.__queryset)

    def in_dict(self):
        """
        Returns the queryset as dictionary

        Returns
        -------
        dict
            queryset as dictionary
        """
        return self.__queryset

    def simple_run(self, output="dataframe"):
        """
        Sends query to EMS API via the regular query call. The regular query call has a size limit
        in the returned data, which is 25000 rows max. Any output that has greater than 25000 rows
        will be truncated. For the query that is expected to return with large data. Please use the
        async_run method.

        Parameters
        ----------
        output: str
            desired output data format. Either "raw" or "dataframe".

        Returns
        -------
        pd.Dataframe
            Returned data for query in Pandas' DataFrame format
        """
        print('Sending a simple query to EMS ...')
        resp_h, content = self._conn.request(
            rtype="POST",
            uri_keys=('database', 'query'),
            uri_args=(self._ems_id, self.__flight.get_database()['id']),
            jsondata=self.__queryset
        )
        print('Done.')

        if output == "raw":
            return content
        elif output == "dataframe":
            return self.__to_dataframe(content)
        else:
            raise ValueError("Requested an unknown output type.")

    def async_run(self, n_row=25000):
        """
        Sends query to EMS API via async-query call. The async-query does not process
        the query as a single batch for a query expecting a large data. You will have
        to call it multiple times. This function do this multiple calls for you.

        Parameters
        ----------
        n_row: int
            batch size of a single async call. Default is 25000.

        Returns
        -------
        pd.DataFrame
            Returned data for query in Pandas' DataFrame format
        """
        print('Sending and opening an async-query to EMS ...', end=' ')
        resp_h, content = self._conn.request(
            rtype="POST",
            uri_keys=('database', 'open_asyncq'),
            uri_args=(self._ems_id, self.__flight.get_database()['id']),
            jsondata=self.__queryset
        )
        if 'id' not in content:
            sys.exit("Opening Async query did not return the query Id.")
        query_id = content['id']
        query_header = content['header']
        print('Done.')

        ctr = 0
        df = None
        while True:
            print(" === Async call: %d ===" % (ctr+1))
            try:
                resp_h, content = self._conn.request(
                    rtype="GET",
                    uri_keys=('database', 'get_asyncq'),
                    uri_args=(
                        self._ems_id,
                        self.__flight.get_database()['id'],
                        query_id,
                        n_row * ctr,
                        n_row * (ctr+1) - 1)
                    )
                content['header'] = query_header
                dff = self.__to_dataframe(content)
            except:
                print("Something's wrong. Returning what has been sent so far.")
                # from pprint import pprint
                # pprint(resp_h)
                # pprint(content)
                return df

            if ctr == 0:
                df = dff
            else:
                df = pd.concat([df, dff], axis=0, join='outer', ignore_index=True)

            print("Received up to %d rows." % df.shape[0])
            if dff.shape[0] < n_row:
                break
            ctr += 1

        print("Done.")
        return df

    def run(self, n_row=25000):
        """
        Sends query to EMS API. It uses either regular or async query call depending on
        the expected size of output data. It supports only Pandas DataFrame as the output
        format.

        Parameters
        ----------
        n_row: int
            batch size of a single async call. Default is 25000.

        Returns
        -------
        pd.DataFrame
            Returned data for query in Pandas' DataFrame format
        """
        Nout = None
        if 'top' in self.__queryset:
            Nout = self.__queryset['top']

        if (Nout is not None) and (Nout <= 25000):
            return self.simple_run(output="dataframe")

        return self.async_run(n_row=n_row)

    def __to_dataframe(self, json_output):
        # Changes Dict (JSON) formatted raw output from the EMS API to Pandas' DataFrame.
        print("Raw JSON output to Pandas dataframe...")
        col = [h['name'] for h in json_output['header']]
        coltypes = [c['type'] for c in self.__columns]
        col_id = [c['id'] for c in self.__columns]
        val = json_output['rows']

        df = pd.DataFrame(data=val, columns=col)

        if df.empty:
            return df

        if self.__queryset['format'] == "display":
            print("Done.")
            return df

        # Do the dirty work of casting a right type for each column of the data
        # Note
        # ====
        # Runway IDs are discrete data but their key-value mapping is not provided
        # because the mapping itself is quite big in size (45K entries). That means
        # the regular routines to handle the discrete data won't work. As a result
        # the discrete data routine has a dirty, custom routine particularly for
        # the runway IDs. What it basically does is to send a separate but redundant
        # query for runway IDs with "queryset$format = display", and then push the
        # this query result at the runway ID column of the original query result.
        # I know this is crappy but it seems the best way I could find.
        for i, cid, cname, ctype in zip(range(len(col)), col_id, col, coltypes):
            # Positional read + isetitem write so that DataFrames with duplicate
            # column names still resolve to a Series and conversion is applied
            # to exactly the i-th column.
            series = df.iloc[:, i]
            try:
                if ctype == 'number':
                    df.isetitem(i, pd.to_numeric(series))
                elif ctype == 'discrete':
                    df.isetitem(i, self.__key_to_val(series, cid))
                elif ctype == 'boolean':
                    df.isetitem(i, series.astype(bool))
                elif ctype == 'dateTime':
                    df.isetitem(i, pd.to_datetime(series, utc=True))
            except (ValueError, TypeError):
                print("Somethings wrong when converting to Pandas DataFrame for column '%s' "
                      "(type: %s)." % (cname, ctype))
        print("Done.")
        return df

    def __key_to_val(self, ds, field_id):
        k_map = self.__flight.list_allvalues(field_id=field_id, in_df=True)
        # Sometimes k_map is a very large table making "replace" operation
        # very slow. Just grap kv-maps subset that are present in the target
        # dataframe
        k_map = k_map[k_map.key.isin(ds.unique())]
        # Change k_map in dict
        km_dict = dict()
        for i, r in k_map.iterrows():
            km_dict[r['key']] = r['value']
        ds = ds.replace(km_dict)
        return ds

    def __get_rwy_id(self, cname):
        # Deprecated
        print("\n --Running a special routine for querying runway IDs. "
              "This will make the querying twice longer.")
        qs = self.__queryset
        qs['format'] = "display"

        resp_h, content = self._conn.request(
            rtype="POST",
            uri_keys=('database', 'query'),
            uri_args=(self._ems_id, self.__flight.get_database()['id']),
            jsondata=qs
        )
        return self.__to_dataframe(content)[cname]

    def update_dbtree(self, *args, **kwargs):
        """
        Update database tree

        Parameters
        ----------
        args:
            database paths
        kwargs
            keyword arguments

        Keyword arguments
        -----------------
        exclude_tree: list
            trees to exclude from database tree

        Returns
        -------
        None
        """
        exclude_tree = kwargs.get("exclude_tree", [])
        self.__flight.update_tree(*args, **{"treetype": "dbtree", "exclude_tree": exclude_tree})

    def update_fieldtree(self, *args, **kwargs):
        """
        Update field tree

        Parameters
        ----------
        args:
            field paths
        kwargs
            keyword arguments

        Keyword arguments
        -----------------
        exclude_tree: list
            trees to exclude from database tree
        exclude_subtrees: bool
            excludes all subtrees if set to True

        Returns
        -------
        None
        """
        exclude_tree = kwargs.get("exclude_tree", [])
        exclude_subtrees = kwargs.get("exclude_subtrees", False)
        self.__flight.update_tree(*args, **{"treetype": "fieldtree",
                                            "exclude_tree": exclude_tree,
                                            "exclude_subtrees": exclude_subtrees})

    def generate_preset_fieldtree(self):
        """
        Creates a default field tree

        Returns
        -------
        None
        """
        self.__flight.make_default_tree()

    def save_metadata(self, file_name=None):
        """
        Saves metadata to the set path

        Parameters
        ----------
        file_name: str
            output path

        Returns
        -------
        None
        """
        self.__flight.save_tree(file_name)

    def load_metadata(self, file_name=None):
        """
        Loads metadata from set path

        Parameters
        ----------
        file_name: str
            input path

        Returns
        -------
        None
        """
        self.__flight.load_tree(file_name)


basic_ops = {
    '==': 'equal',
    '!=': 'notEqual',
    '<': 'lessThan',
    '<=': 'lessThanOrEqual',
    '>': 'greaterThan',
    '>=': 'greaterThanOrEqual'
}
between_ops = {
    '<': {
        '<': 'betweenExclusive'
    },
    '<=': {
        '<=': 'betweenInclusive'
    },
    '>': {
        '>': 'notBetweenExclusive'
    },
    '>=': {
        '>=': 'notBetweenInclusive'
    }
}
sp_ops = OrderedDict([
    ('not in', 'notIn'),
    ('in', 'in'),
    ('is null', 'isNull'),
    ('is not null', 'isNotNull')
])

# '=Null': 'isNull', '!=Null': 'isNotNull', 'and': 'And', 'or': 'Or', 'in': 'in', 'not in': 'notIn'


def _filter_fmt1(op, *args):
    fltr = {
        "type": "filter",
        "value": {
            "operator": op,
            "args": []
        }
    }
    for x in args:
        fltr['value']['args'].append({'type': x['type'], 'value': x['value']})
    return fltr


def _boolean_filter(op, d):
    # Between filters unsupported
    if len(d) > 2:
        raise ValueError('Unsupported conditional operator for boolean field.')
    fld_info = d[0]
    if len(d) == 1:
        if op.strip().lower() == 'is null':
            t_op = 'isNull'
        elif op.strip().lower() == 'is not null':
            t_op = 'isNotNull'
    elif len(d) == 2:
        val_info = d[1]
        if not isinstance(val_info['value'], bool):
            raise ValueError("%s: use a boolean value." % val_info['value'])
        if op == "==":
            t_op = 'is'+str(val_info['value'])
        elif op == "!=":
            t_op = 'is'+str(not val_info['value'])
        else:
            raise ValueError("Conditional operator %s is given. "
                             "Booleans should be only with boolean operators." % op)
    fltr = _filter_fmt1(t_op, fld_info)
    return fltr


def _discrete_filter(op, d, flt):
    fld_info = d[0]

    # Between filters unsupported
    if isinstance(op, list):
        raise ValueError("%s: Unsupported conditional operators for discrete field type." % op)

    if op in list(basic_ops.keys()):
        # Single input basic conditional operation
        t_op = basic_ops[op]
        val_info = d[1]
        vid = flt.get_value_id(val_info['value'], field_id=fld_info['value'])
        val_info['value'] = vid
        fltr = _filter_fmt1(t_op, fld_info, val_info)
    elif op.strip() in sp_ops.keys():
        t_op = sp_ops[op]
        val_list = [
            {'type': x['type'], 'value': flt.get_value_id(x['value'], field_id=fld_info['value'])}
            for x in d[1:]
        ]
        inp = [fld_info] + val_list
        fltr = _filter_fmt1(t_op, *inp)
    else:
        raise ValueError("%s: Unsupported conditional operator for discrete field type." % op)
    return fltr


def _number_filter(op, d):
    if isinstance(op, string_types):
        if op in basic_ops.keys():
            t_op = basic_ops[op]
            fltr = _filter_fmt1(t_op, d[0], d[1])
        elif op in sp_ops.keys():
            t_op = sp_ops[op]
            fltr = _filter_fmt1(t_op, *d)
        else:
            raise ValueError("%s: Unsupported conditional operator for number field type." % op)
    elif isinstance(op, list):
        if op[1] in between_ops[op[0]]:
            t_op = between_ops[op[0]][op[1]]
            fltr = _filter_fmt1(t_op, d[0], d[1], d[2])
        else:
            raise ValueError("%s: Unsupported conditional operators for number field type." % op)
    return fltr


def _string_filter(op, d):
    # Between filters unsupported
    if isinstance(op, list):
        raise ValueError("%s: Unsupported conditional operators for string field type." % op)

    if op in ["==", "!="]:
        t_op = basic_ops[op]
        fltr = _filter_fmt1(t_op, d[0], d[1])
    elif op.strip() in sp_ops.keys():
        t_op = sp_ops[op]
        fltr = _filter_fmt1(t_op, *d)
    else:
        raise ValueError("%s: Unsupported conditional operator for string field type." % op)

    return fltr


def _datetime_filter(op, d):
    basic_date_ops = {
        "<": "dateTimeBefore",
        ">=": "dateTimeOnAfter",
    }
    sp_date_ops = {
        "is null": "isNull",
        "is not null": "isNotNull"
    }

    # Between filters unsupported
    if isinstance(op, list):
        raise ValueError("%s: Unsupported conditional operators for datetime field type." % op)

    if op in list(basic_date_ops.keys()):
        t_op = basic_date_ops[op]
        fltr = _filter_fmt1(t_op, *d)
        # Additional json attribute to specify this is UTC time
        fltr['value']['args'].append({'type': 'constant', 'value': 'Utc'})
    elif op in list(sp_date_ops.keys()):
        t_op = sp_date_ops[op]
        fltr = _filter_fmt1(t_op, *d)
    else:
        raise ValueError("%s: Unsupported conditional operator for datetime field type." % op)

    return fltr
