from __future__ import absolute_import
from __future__ import print_function

import json
import sys
from builtins import str
from builtins import zip
from collections import OrderedDict

import pandas as pd
from future.utils import string_types

from emspy.query import *
from .query import Query


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

    def deselect(self, *args):
        """
        Removes fields from the query

        Parameters
        ----------
        args:
            fields to remove

        Returns
        -------
        None
        """
        fields = self.__flight.search_fields(*args)
        if not isinstance(fields, list):
            fields = [fields]
        for field in fields:
            matching_entries = [d for d in self.__queryset['select'] if d['fieldId'] == field['id']]
            for entry in matching_entries:
                self.__queryset['select'].remove(entry)
            self.__columns.remove(field)

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
            try:
                if ctype == 'number':
                    df.iloc[:, i] = pd.to_numeric(df.iloc[:, i])
                elif ctype == 'discrete':
                    df.iloc[:, i] = self.__key_to_val(df.iloc[:, i], cid)
                    # k_map = self.__flight.list_allvalues(field_id=cid, in_dict=True)
                    # if len(k_map) == 0:
                    #     df[cname] = self.__get_rwy_id(cname)
                    # else:
                    #     df = df.replace({cname: k_map})
                elif ctype == 'boolean':
                    df.iloc[:, i] = df.iloc[:, i].astype(bool)
                elif ctype == 'dateTime':
                    df.iloc[:, i] = pd.to_datetime(df.iloc[:, i], utc=True)
            except ValueError:
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
