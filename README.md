# emsPy
A Python Wrapper of EMS API. There is also a R wrapper for EMS API. If you are interest in the R version, please visit <https://github.com/ge-flight-analytics/Rems>. The goal of this project is provide a way to bring EMS data in Python environment via the EMS's RESTful API. The project is still in an early alpha stage, so is not guarranteed working reliably nor well documented. I'll beef up the documentation as soon as possible.

Any contribution is welcome!

Dependency: 
* numpy >= 1.11 
* pandas >= 0.18
* future >= 0.16

## v0.2.1 Changes
* Packed as an installable package.
* emsPy is now Python 3 compatible.
* **Caution: this version is possibly buggy, especially in the installation and Python 3 usage.** I have not gone through many tests yet in Python 3. 


## Installation
1. Download or clone emsPy. If downloaded, unzip the compressed file.
2. Go to the folder that you unzipped or git-cloned, where you can find `setup.py` file.
3. At the folder, open the command prompt window and run `pip install .` For dev-mode, run `pip install -e .`


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

c = Connection("efoqa_usrname", "efoqa_password", proxies = proxies, server = "prod")

```
With optional `server` argument, you can select one of the currently available EMS API servers, which are:
* "prod" (default)
* "cluster" (clustered production version)
* "stable" (stable test version)
* "beta" 
* "nightly"

## Fight Querying

### Instantiate Query 

The following example instantiates a flight-specific query object that will send queries to the EMS 9 system. 
```python
from emspy.query import FltQuery

query = FltQuery(c, 'ems9', data_file = 'metadata.db')
```
where optional `data_file` input specifies the SQLite file that will be used to read/write the meta data in the local machine. If there is no file with a specified file name, a new db file will be created. If no file name is passed, it will generate a db file in the default location (emspy/data).



### EMS Database Setup
The FDW Flights database is one of the frequently used databases. In order to select it as your database for querying, you can simply run the following line.

```python
query.set_database("fdw flights")
```

In EMS system, all databases & data fields are organized in hierarchical tree structures. In order to use a database that is not the FDW Flights, you need to tell the query object where in the EMS DB tree your database is at. You can explore the EMS database tree from [EMS Online](http://fas.efoqa.com/Docs/Rest/Demos/DataSources). The following example specifies the location of one of the Event databases in the DB tree and then set the Event database that you want to use:

```python
query.update_dbtree("fdw", "events", "standard", "p0")
query.set_database("p0: library flight safety events")
```

These code lines first send queries to find the database-groups path, **FDW &rarr; APM Events &rarr; Standard Library Profiles &rarr; P0: Library Flight Safety Events**, and then select the "P0: Library Flight Safety Events" database that is located at the specified path. For a complete example, please check on [this Gist](https://gist.github.com/KMoon01/d82324594c975104c763140a54133565).

### Data Fields
Similar to the databases, the EMS data fields are organized in a tree structure so the steps are almost identical except that you use `update_fieldtree(...)` method in order to march through the tree branches.

Before calling the `update_fieldtree(...)`, you can call `update_preset_fieldtree()` method to load a basic tree with fields belonging to the following field groups:
* Flight Information
* Aircraft Information
* Navigation Information

Let say you have selected the FDW Flights database. The following code lines will query for the meta-data of basic data fields, and then some of the data fields in the Profile 301 in EMS9. 

```python
# Let the query object load preset data fields that are frequently used
query.generate_preset_fieldtree()

# Load other data fields that you want to use
query.update_fieldtree("profiles", "standard", "block-cost", "p301",
                       "measured", "ground operations (before takeoff)")
```
The `update_fieldtree(...)` above queries the meta-data of all measurements located at the path, **Profiles &rarr; Standard Library Profiles &rarr; Block-Cost Model &rarr; P301: Block-Cost Model Planned Fuel Setup and Tests &rarr; Measured Items &rarr;Ground Operations (before takeoff)** in EMS Explorer.

**Caution**: the process of adding a subtree usually requires a very large number of recursive RESTful API calls which take quite a long time. Please try to specify the subtree to as low level as possible to avoid a long processing time.

As you may noticed in the example codes, you can specify a data entity by the string fraction of its full name. The "key words" of the entity name follows this rule:
* Case insensitive
* Keyword can be a single word or multiple consecutive words that are found in the full name string
* Keyword should uniquely specify a single data entity among all children under their parent database group
* Regular expression is not supported


### Saving Meta-Data
Finally, you can save your the meta-data of the database/data trees for later uses. Once you save it, you can go directly call `set_database(...)` without querying the same meta-data for later executions. However, you will have to update trees again if any of the data entities are modified at the EMS-system side.

```python
# This will save the meta-data into demo.db file, in SQLite format
query.save_metadata()
```

### Select
As a next step, you will start make an actual query. The `select(...)` method is used to select what will be the columns of the returned data for your query. Following is an example:

```python
query.select("flight date", 
             "customer id", 
             "takeoff valid", 
             "takeoff airport iata code")
```

The passed data fields must be part of the data fields in your data tree. 

You need to make a separate select call if you want to add a field with aggregation applied.

```python
query.select("P301: duration from first indication of engines running to start", 
             aggregate="avg")
```
Supported aggregation functions are:
* avg
* count
* max
* min
* stdev
* sum
* var

You may want to define grouping, which is described in the next section, when you want to apply an aggregation function.

`select(...)` method accepts the keywords too, and even a combination of keywords to specify the parent directories of the fields in the data tree. For example, the following keywords are all valid to select "Flight Date (Exact)" for query:
- Search by a consecutive substring. The method returns a match with the shortest field name if there are multiple match.
    - Ex) "flight date"
- Search by exact name. 
    - Ex) "flight date (exact)"
- Field name keyword along with multiple keywords for the names of upstream field groups (i.e., directories). 
    - Ex) ("flight info", "date (exact)")


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

EMS API supports two different query executions which are regular and async queries. The regular query has a data size limit for the output data, which is 25000 rows. On the other hand, the async query is able to handle large output data by letting you send repeated requests for mini batches of the large output data.

The `run()` method takes care of the repeated async requests for a query whose returning data is expected to be large.

The batch data size for the async request is set 25,000 rows as default (which is the maximum). If you want to change this size,
```python
# Set the batch size as 20,000 rows per request
df = query.run(n_row = 20000)
``` 

## Querying Time-Series Data
You can query data of time-series parameters with respect to individual flight records. Below is a simple example code that sends a flight query first in order to retrieve a set of flights and then sends queries to get some of the time-series parameters for each of these flights.

```python
# Flight query with an APM profile. It will return data for 10 flights
fq = FltQuery(c, "ems9", data_file = "demo.db")
fq.set_database("fdw flights")
# If you reuse the meta-data, you don't need to update db/field trees.

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

tsq = TSeriesQuery(c, "ems9", data_file = "demo.db")
tsq.select(
  "baro-corrected altitude", 
  "airspeed (calibrated; 1 or only)", 
  "ground speed (best avail)",
    "egt (left inbd eng)", 
    "egt (right inbd eng)", 
    "N1 (left inbd eng) (%)", 
    "N1 (right inbd eng) (%)")

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
