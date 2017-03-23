from emspy.query import *
from query import Query

import cPickle
import warnings
import pandas as pd
import numpy as np


class TSeriesQuery(Query):


    def __init__(self, conn, ems_name, data_file = None):

        Query.__init__(self, conn, ems_name)
        self._init_assets(data_file)
        self.reset()


    def _init_assets(self, data_file):

        # Query._init_assets(self)
        self.__analytic = Analytic(self._conn, self._ems_id, data_file)


    def reset(self):

        self.__columns  = list()
        self.__queryset = {'select':[]}


    def select(self, *args):

        keywords   = args
        save_table = False

        for kw in keywords:
            # Get the param from param table
            prm = self.__analytic.get_param(kw)
            if prm['id'] == "":
                # If the param's not found, call EMS API
                res_df = self.__analytic.search_param(kw, in_df = True)
                res_df['ems_id'] = self._ems_id
                # The first one is with the shortest name string. Pick that.
                prm = res_df.iloc[0,:].to_dict()
                # Add the new parameters to the param table for later uses
                self.__analytic._param_table = self.__analytic._param_table.append(res_df, ignore_index = True)
                save_table = True

            # Put the param into JSON query string
            self.__queryset['select'].append({'analyticId': prm['id']})
            # Just in case you want to check what params are selected
            self.__columns.append(prm)
        if save_table:
            self.__analytic._save_paramtable()


    def range(self, start, end):

        self.__queryset['start'] = start
        self.__queryset['end']   = end


    def timepoint(self, tpoint):
        if type(tpoint) == np.ndarray:
            tpoint = tpoint.tolist()
        self.__queryset['offsets'] = tpoint


    def run(self, flight, start = None, end = None, timestep = 1.0, timepoint = None):

        if start is None:
            start = 0.0

        if end is None:
            end = self.flight_duration(flight)

        if timestep is not None:
            timepoint = np.arange(start, end, timestep)

        if timepoint is not None:
            self.timepoint(timepoint)
        else:
            self.range(start, end)

        resp_h, content = self._conn.request( uri_keys = ("analytic", "query"),
                                              uri_args = (self._ems_id, flight),
                                              jsondata = self.__queryset)

        if content.has_key('message'):
            sys.exit('API query for flight %d was unsuccessful.\nHere is the message from API: %s' % (flight, content['message']))
        
        # Put the data in Pandas DataFrame
        df = pd.DataFrame({"Time (sec)": content['offsets']})

        for i, prm in enumerate(self.__columns):
            df[prm['name']] = content['results'][i]['values']

        ## "\r\x1b[K" is overwrite print.
        return df


    def multi_run(self, flight, start = None, end = None, timestep=1.0, timepoint = None, save_file = None):

        res       = list()
        attr_flag = False
        if isinstance(flight, pd.DataFrame):
            FR = flight["Flight Record"]
            attr_flag = True
        else:
            FR = flight

        # param processing
        if start is None:       start       = [None]*len(FR)
        if end   is None:       end         = [None]*len(FR)
        if not hasattr(timestep, "__len__"):
            timestep = [timestep]*len(FR)
        if timepoint is None:   
            timepoint   = [None]*len(FR)
        else: 
            warnings.warn("Time points are not yet supported. The given time points will be ignored.")
            timepoint   = [None]*len(FR)

        print('\n=== Start running time-series data querying for %d flights ===\n' % len(FR))
        for i, fr in enumerate(FR):
            print '\r\x1b[K%d / %d: FR %d' % (i+1, len(FR), fr),
            i_res = dict()
            if attr_flag:
                i_res['flt_data'] = flight.iloc[i,:].to_dict()
            else:
                i_res['flt_data'] = {'Flight Record': fr}
            i_res['ts_data'] = self.run(fr, start[i], end[i], timepoint[i])
            res.append(i_res)

            if save_file is not None:
                cPickle.dump(res, open(save_file, 'wb'))
        print 'Done'

        return res


    def flight_duration(self, flight, unit = "second"):

        p = self.__analytic.get_param("hours of data (hours)")
        if p["id"] == "":
            res_df = self.__analytic.search_param("hours of data (hours)", in_df = True)
            p      = res_df.iloc[0].to_dict()
            self.__analytic._param_table = self.__analytic._param_table.append(res_df, ignore_index = True)
            self.__analytic._save_paramtable()
        q = {
            "select": [{"analyticId": p["id"]}],
            "size": 1
        }

        resp_h, content = self._conn.request( uri_keys = ("analytic", "query"),
                                              uri_args = (self._ems_id, flight),
                                              jsondata = q)
        if content.has_key('message'):
            sys.exit('API query for flight %d, parameter = "%s" was unsuccessful.\nHere is the message from API: %s' % (flight, p['name'], content['message']))
        fl_len = content['results'][0]['values'][0]

        if unit == "second":
            t = fl_len * 60 * 60
        elif unit == "minute":
            t = fl_len * 60
        elif unit == "hour":
            t = fl_len
        else:
            sys.exit("Unrecognizable time unit (%s)." % unit)
        return t
