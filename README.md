# emsPy
A Python Wrapper of EMS API. There is also a R wrapper for EMS API. If you are interest in the R version, please visit <https://github.build.ge.com/212401522/Rems>. The goal of this project is provide a way to bring EMS data in Python environment via the EMS's RESTful API. The project is still in an early alpha stage, so is not guarranteed working reliably nor well documented. I'll beef up the documentation as soon as possible.

Any contribution is welcome!

Dependency: 
* numpy 1.11
* pandas 0.18
* networkx 1.11

## Installation
Right now, I've not yet packed this into an installation package. Just download or clone emsPy in a local directory and work on there. I'll make it as an installable package soon.

## Make an EMS API Connection

The optional proxy setting can be passed to the EMS connection object with the following format:
```python
proxies = {
    'http': 'http://{prxy_usrname}:{prxy_password}@{proxy_server_address}:{port}',
    'https': 'https://{prxy_usrname}:{prxy_password}@{proxy_server_address}:{port}'
}
```

```python
from emspy import Connection

c = Connection("efoqa_usrname", "efoqa_password", proxies = proxies)

```

## Fight Querying

### Instantiate Query 


```python
from emspy.query import FltQuery

query = FltQuery(c, ems_name = 'ems9')
```

Current limitations:
- The current query object only support the FDW Flight data source, which seems to be reasonable for POC functionality.
- Right now a Query object can be instantiated only with a single EMS system connection. I think a Query object with multi-EMS connection could be quite useful for data analysts who want to do study pseudo-global patterns.
    - Ex) query = Query(c, ems_name = ['ems9', 'ems10', 'ems11']) 


### EMS Database Setup

The EMS system handles with data fields based on a hierarchical tree structure. This field tree manages mappings between names and field IDs as well as the field groups of fields. In order to send query via EMS API, the emsPy package will automatically generate a data file for the static, frequently used part of the field tree and load it as default. This bare field tree includes fields of the following field groups:

* Flight Information (sub-field groups Processing and Profile 16 Extra Data were excluded)
* Aircraft Information
* Flight Review
* Data Information
* Navigation Information
* Weather Information

In case that you want to query with fields that are not included in this default, stripped-down data tree, you'll have to add the field group where your fields belongs to and update your data field tree. For example, if you want to add a field group branch such as Profiles --> Standard Library Profiles --> Block-Cost Model --> P301: Block-Cost Model Planned Fuel Setup and Tests --> Measured Items --> Ground Operations (before takeoff), the execution of the following method will add the fields and their related subtree structure to the basic tree structure. You can use either the full name or just a fraction of consequtive keywords of each field group. The keyword is case insensitive.

**Caution**: the process of adding a subtree usually requires a very large number of recursive RESTful API calls which take quite a long time. Please try to specify the subtree to as low level as possible to avoid a long processing time.

```python
query.update_datatree("profiles", "standard", "block-cost", "p301", 
                      "measured", "ground operations (before takeoff)")
```
You can save your data tree for later uses: 
```python
## This will replace the default data tree file with the new one and save 
## at the package root/data
query.save_datatree()
```
If you saved the data tree with default file location, the save data tree will be automatically reloaded when you intantiate a new Query object. 

In case you want to save the data tree locally, you can call `save_datatreee` method and specify your own choice of a file name with a proper path. For example, the following will save the data tree at your working directory.
```python
query.save_datatree('my_datatree.pkl')
```
If you saved the data tree in a local file, you will have to explicitly load the file to reuse the saved datatree.
```python
query.load_datatree('my_datatree.pkl')
```
### Select
Let's first go with "select". You can select the EMS data fields by keywords of their names as long as the keyword searches a field. For example, the select method finds you the field "Flight Date (Exact)" by passing three different search approaches:
- Search by a consecutive substring. The method returns a match with the shortest field name if there are multiple match.
    - Ex) "flight date"
- Search by exact name. 
    - Ex) "flight date (exact)"
- Field name keyword along with multiple keywords for the names of upstream field groups. 
    - Ex) ("flight info", "date (exact)")

The keyword is case-insensitive.


```python
query.select("flight date", 
             "customer id", 
             "takeoff valid", 
             "takeoff airport iata code")
```

You need to make a separate select call if you want to add a field with aggregation applied


```python
query.select("P301: duration from first indication of engines running to start", 
             aggregate="avg")
```

### Group by & Order by
Similarly, you can pass the grouping and ordering condition:


```python
query.group_by("flight date",
               "customer id",
               "takeoff valid",
               "takeoff airport iata code")

query.order_by("flight date")
# the ascending order is default. You can pass a descending order by optional input:
#     query.order_by("flight date", order="desc")
```

### Filtering
Currently the following conditional operators are supported with respect to the data field types:
- Number: "==", "!=", "<", "<=", ">", ">="
- Discrete: "==", "!=", "in", "not in" (Filtering condition made with value, not discrete integer key)
- Boolean: "==", "!="
- String: "==", "!=", "in", "not in"
- Datetime: ">=", "<"

Following is the example:


```python
query.filter("'flight date' >= '2016-1-1'")
query.filter("'takeoff valid' == True")
# Discrete field filtering is pretty much the same as string filtering.
query.filter("'customer id' in ['CQH','EVA']") 
query.filter("'takeoff airport iata code' == 'KUL'")
```

The current filter method has the following limitation:
- Single filtering condition for each filter method call
- Filtering conditions are combined only by "AND" relationship
- The field keyword must be at left-hand side of a conditional expression
- No support of NULL value filtering, which is being worked on now
- The datetime condition should be only with the ISO8601 format

### ETC.
You can pass additional attributes supported by EMS query:


```python
# Returns only the distinct rows. Turned on as default
query.distinct(True)

# If you want get top N the rows of the output data in response to the query, 
query.get_top(5000)

# This is optional. If you don't set this value, all output data will be returned.
```

### Viewing JSON Translation of Your Query
You can check on the resulting JSON string of the translated query using the following method calls.


```python
# Returns JSON string
# print query.in_json()

# View in Python's native Dictionary form 
from pprint import pprint # This gives you a prettier print

print("\n")
pprint(query.in_dict())
```

    
    
    {'distinct': True,
     'filter': {'args': [{'type': 'filter',
                          'value': {'args': [{'type': 'field',
                                              'value': u'[-hub-][field][[[ems-core][entity-type][foqa-flights]][[ems-core][base-field][flight.exact-date]]]'},
                                             {'type': 'constant',
                                              'value': '2016-1-1'},
                                             {'type': 'constant',
                                              'value': 'Utc'}],
                                    'operator': 'dateTimeOnAfter'}},
                         {'type': 'filter',
                          'value': {'args': [{'type': 'field',
                                              'value': u'[-hub-][field][[[ems-core][entity-type][foqa-flights]][[ems-core][base-field][flight.exist-takeoff]]]'}],
                                    'operator': 'isTrue'}},
                         {'type': 'filter',
                          'value': {'args': [{'type': 'field',
                                              'value': u'[-hub-][field][[[ems-core][entity-type][foqa-flights]][[ems-fcs][base-field][fdw-flight-extra.customer]]]'},
                                             {'type': 'constant',
                                              'value': 18},
                                             {'type': 'constant',
                                              'value': 11}],
                                    'operator': 'in'}},
                         {'type': 'filter',
                          'value': {'args': [{'type': 'field',
                                              'value': u'[-hub-][field][[[ems-core][entity-type][foqa-flights]][[[nav][type-link][airport-takeoff * foqa-flights]]][[nav][base-field][nav-airport.iata-code]]]'},
                                             {'type': 'constant',
                                              'value': 'KUL'}],
                                    'operator': 'equal'}}],
                'operator': 'and'},
     'format': 'display',
     'groupBy': [{'fieldId': u'[-hub-][field][[[ems-core][entity-type][foqa-flights]][[ems-core][base-field][flight.exact-date]]]'},
                 {'fieldId': u'[-hub-][field][[[ems-core][entity-type][foqa-flights]][[ems-fcs][base-field][fdw-flight-extra.customer]]]'},
                 {'fieldId': u'[-hub-][field][[[ems-core][entity-type][foqa-flights]][[ems-core][base-field][flight.exist-takeoff]]]'},
                 {'fieldId': u'[-hub-][field][[[ems-core][entity-type][foqa-flights]][[[nav][type-link][airport-takeoff * foqa-flights]]][[nav][base-field][nav-airport.iata-code]]]'}],
     'orderBy': [{'aggregate': 'none',
                  'fieldId': u'[-hub-][field][[[ems-core][entity-type][foqa-flights]][[ems-core][base-field][flight.exact-date]]]',
                  'order': 'asc'}],
     'select': [{'aggregate': 'none',
                 'fieldId': u'[-hub-][field][[[ems-core][entity-type][foqa-flights]][[ems-core][base-field][flight.exact-date]]]'},
                {'aggregate': 'none',
                 'fieldId': u'[-hub-][field][[[ems-core][entity-type][foqa-flights]][[ems-fcs][base-field][fdw-flight-extra.customer]]]'},
                {'aggregate': 'none',
                 'fieldId': u'[-hub-][field][[[ems-core][entity-type][foqa-flights]][[ems-core][base-field][flight.exist-takeoff]]]'},
                {'aggregate': 'none',
                 'fieldId': u'[-hub-][field][[[ems-core][entity-type][foqa-flights]][[[nav][type-link][airport-takeoff * foqa-flights]]][[nav][base-field][nav-airport.iata-code]]]'},
                {'aggregate': 'avg',
                 'fieldId': u'[-hub-][field][[[ems-core][entity-type][foqa-flights]][[ems-apm][flight-field][msmt:profile-cbaa5341ca674914a6ceccd6f498bffc:msmt-0d7fe63d6863451a9c663a09fd780985]]]'}],
     'top': 5000}
    

### Run Query and Retrieve Data
You can finally send the query to the EMS system and get the data. The output data is returned in Pandas' DataFrame object.



```python
df = query.run()

# This will return your data in Pandas dataframe format
```

EMS API supports two different query executions which are regular and async queries. The regular query has a data size limit for the output data, which is 5000 rows. On the other hand, the async query is able to handle large output data by letting you send repeated requests for mini batches of the large output data.

The `run()` method takes care of the repeated async request for a query whose returning data is expected to be large.

The batch data size for the async request is set 10000 rows as default. If you want to change this size,
```python
# Set the batch size as 20,000 rows per request
df = query.run(n_row = 20000)
``` 

## Querying Time-Series Data
You can query data of time-series parameters with respect to individual flight records. Below is a simple example code that sends a flight query first in order to retrieve a set of flights and then sends queries to get some of the time-series parameters for each of these flights.

```python
# Flight query with an APM profile. It will return data for 10 flights
fq = FltQuery(c, "ems9")

fq.load_datatree("stat_taxi_datatree.pkl")

fq.select(
  "customer id", "flight record", "airframe", "flight date (exact)",
    "takeoff airport code", "takeoff airport icao code", "takeoff runway id",
    "takeoff airport longitude", "takeoff airport latitude",
    "p185: processed date", "p185: oooi pushback hour gmt",
    "p185: oooi pushback hour solar local",
    "p185: total fuel burned from first indication of engines running to start of takeoff (kg)")
fq.order_by("flight record", order='desc')
fq.get_top(10)
fq.filter("'p185: processing state' == 'Succeeded'")
flt = fq.run()

# === Run time series query for flights ===

tsq = TSeriesQuery(c, "ems9")
tsq.select(
  "baro-corrected altitude", 
  "airspeed (calibrated; 1 or only)", 
  "ground speed (best avail)",
    "egt (left inbd eng)", 
    "egt (right inbd eng)", 
    "N1 (left inbd eng)", 
    "N1 (right inbd eng)")

# Run querying multiple flights at once. Start time = 0, end time = 15 mins (900 secs) for all flights. 
# A better use case is that those start/end times are fed by timepoint measurements of your APM profile.
res_dat = tsq.multi_run(flt, start = [0]*flt.shape[0], end = [15*60]*flt.shape[0])
```

The inputs to function `multi_run(...)` are:
* flt  : a vector of Flight Records or flight data in Pandas DataFrame format. The dataframe should have a column of flight records with its column name "Flight Record"
* start: a list-like object defining the starting times (secs) of the timepoints for individual flights. The vector length must be the same as the number of flight records
* end  : a list-like object defining the end times (secs) of the timepoints for individual flights. The vector length must be the same as the number of flight records
* timestep: a list-like object defining the size of timesteps in seconds for individual flights. Default is set 1 second. If you set "None", it will use the parameters' own default timesteps. The vector length must be the same as the number of flight records

The output will be Python dictionary object which contains the following data:
* flt_data : Dictionary. Copy of the flight data for each flight
* ts_data  : Pandas DataFrame. the time series data for each flight

In case you just want to query for a single flight, `run(...)` function will be better suited. Below is an example of time-series querying for a single flight.

```python
res_dat = tsq.run(1901112, start=0, end=900)
```
This function will return a Pandas DataFrame that contains timepoints from 0 to 900 secs and corresponding values for selected parameters. You can also pass a timestep as an optional argument. Default timestep is set 1.0 sec.
