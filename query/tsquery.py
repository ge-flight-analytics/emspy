from emspy.query import *
from query import Query

import warnings
import pandas as pd


class TSeriesQuery(Query):


    def __init__(self, conn, ems_name, new_data = False):

        Query.__init__(self, conn, ems_name)
        self._init_assets(new_data)
        self.reset()


    def _init_assets(self, new_data):

        # Query._init_assets(self)
        self.__analytic = Analytic(self._conn, self._ems_id, new_data)


    def reset(self):

        self.__columns  = list()
        self.__queryset = dict()


    def select(self, *args):

        keywords   = args
        save_table = False

        for kw in keywords:
            # Get the param from param table
            prm = self.__analytic.get_param(kw)
            if prm['id'] == "":
                # If the param's not found, call EMS API
                res_df = self.__analytic.search_param(kw, in_dataframe = True)
                # The first one is with the shortest name string. Pick that.
                prm = res_df.iloc[0,:].to_dict()
                # Add the new parameters to the param table for later uses
                self.__analytic._param_table = self.__analytic._param_table.append(res_df, ignore_index = True)
                save_table = True
            self.__columns.append(prm)
        if save_table:
            self.__analytic._save_paramtable()


    def range(self, start, end):

        self.__queryset['start'] = start
        self.__queryset['end']   = end


    def run(self, flight, start = None, end = None, timepoint = None):

        for i, p in enumerate(self.__columns):
            if (start is not None) and (end is not None):
                self.range(start, end)
            if timepoint is not None:
                warnings.warn("Defining time points is not yet supported. The given time points will be ignored.")
            q = self.__queryset.copy()
            q['select'] = [{'analyticId': p['id']}]
            resp_h, content = self._conn.request( uri_keys = ("analytic", "query"),
                                                  uri_args = (self._ems_id, flight),
                                                  jsondata = q)
            if content.has_key('message'):
                sys.exit('API query for flight %d, parameter = "%s" was unsuccessful.\nHere is the message from API: %s' % (flight, p['name'], content['message']))
            if i == 0:
                df = pd.DataFrame({"Time (sec)": content['offsets']})
            df[p['name']] = content['results'][0]['values']
        return df


    def multi_run(self, flight, start = None, end = None, timepoint = None):

        res       = list()
        attr_flag = False
        if isinstance(flight, pd.DataFrame):
            FR = flight["Flight Record"]
            attr_flag = True
        else:
            FR = flight
        if start is None:       start       = [None]*len(FR)
        if end is None:         end         = [None]*len(FR)
        if timepoint is None:   
            timepoint   = [None]*len(FR)
        else: 
            warnings.warn("Time points are not yet supported. The given time points will be ignored.")
            timepoint   = [None]*len(FR)

        print('\n=== Start running time-series data querying for %d flights ===\n' % len(FR))
        for i, fr in enumerate(FR):
            print '%d / %d: FR %d' % (i+1, len(FR), fr)
            i_res = dict()
            if attr_flag:
                i_res['flt_data'] = flight.iloc[i,:].to_dict()
            else:
                i_res['flt_data'] = {'Flight Record': fr}
            i_res['ts_data'] = self.run(fr, start[i], end[i], timepoint[i])
            res.append(i_res)
        return res

