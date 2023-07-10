# emsPy
A Python Wrapper of EMS API. There is also a R wrapper for EMS API. If you are interest in the R version, please visit <https://github.com/ge-flight-analytics/Rems>. The goal of this project is provide a way to bring EMS data in Python environment via the EMS's RESTful API.

## Branches

* New work is completed in the `master` branch of this repository and may contain breaking changes. Pull requests should be made to this branch.
* Old stable functionality exists in the `v0.2.1` branch and should never change except for minor fixes.
* Releases are publicly available on `PyPi` using semantic versioning.

## Installation

Install from the package index:

```
pip install emspy
```

Alternatively, the package can be installed from the git repository or a zip package:

1. Download or clone emsPy. If downloaded, unzip the compressed file.
2. Go to the folder that you unzipped or git-cloned, where you can find `setup.py` file.
3. At the folder, open the command prompt window and run `pip install .` For dev-mode, run `pip install -e .`

## Make an EMS API Connection

The optional proxy setting can be passed to the EMS connection object with the following format:
```python
proxies = {
    'http': 'http://{proxy_server_address}:{port}',
    'https': 'https://{proxy_server_address}:{port}'
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

For servers hosted locally or in Azure, the server_url argument should be used instead of the server argument. This argument should be of the format "<server address>/api". For example, if the server hosting the API is https://abc-api.us.efoqa.com, then the connection object would look like this

```python
from emspy import Connection

c = Connection("usrname", "password", proxies=proxies, server_url="https://abc-api.us.efoqa.com/api")

```

## Fight Querying

### Instantiate Query 

The following example instantiates a flight-specific query object that will send queries to the EMS 9 system. 
```python
from emspy.query import FltQuery

query = FltQuery(c, 'ems9', data_file = 'metadata.db')
```
where optional `data_file` input specifies the SQLite file that will be used to read/write the meta data in the local machine. If there is no file with a specified file name, a new db file will be created. If no file name is passed, it will generate a db file in the default location (emspy/data).  If None is specified, no db file will be created.  



### EMS Database Setup
The FDW Flights database is one of the frequently used databases. In order to select it as your database for querying, you can simply run the following line.

```python
query.set_database("fdw flights")
```

In EMS system, all databases & data fields are organized in hierarchical tree structures. In order to use a database that is not the FDW Flights, you need to tell the query object where in the EMS DB tree your database is at. You can explore the EMS database tree from [EMS Online](https://fas.efoqa.com/Docs/Rest/Demos/DataSources). The following example specifies the location of one of the Event databases in the DB tree and then set the Event database that you want to use:

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

By default, the `update_fieldtree(...)` method will load ALL subfolders of the last item in the path. If there are a lot of subfolders, this can take a long time and is likely not necessary. the keyword arguments `exclude_subtrees` and `exclude_tree` can be used to prevent all or some of the subtreed to be loaded. To exlude all subtrees set the `exclude_subtrees=FALSE`
```python
query.update_fieldtree("profiles", "standard", "block-cost", "p301",
                       "measured", "ground operations (before takeoff)", exclude_subtrees=FALSE)
```
To only exclude a list of subtrees use `exclude_tree=[...]`
```python
query.update_fieldtree('Flight Information', exclude_tree=['Processing', 'FlightPulse'])
```

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

To avoid name collisions, it is also possible to pass a tuple into the `select()` method with the full path to the field of interest. All but the last elements will represent a folder in the path, and the last element is the field itself. For example, if you want to select the "Takeoff Airport Code" field in the "Navigation Information\Takeoff\Airport" folder. your select statement would look like this
```python
query.select( ('Navigation Information', 'Takeoff', 'Airport', 'Takeoff Airport Code') )
```

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

### Ignoring Local Cache
When running a Time-Series query, emapy create a local cache of found parameters to be re-used on later calls. Sometimes this can cause the wrong parameter to be selected if a parameter with a similar name to the one you want is already in the cache. to force emspy to search for parameters from ems every time and ignore the cache, you can add the force_search=True option in the select() method like this
```python
tsq.select(
  "baro-corrected altitude", 
  "airspeed (calibrated; 1 or only)", 
  "ground speed (best avail)",
  force_search=True)
```

### Loading Analytics from a Parameter Set
If you want to load all Analytics from a parameters set, you can use the `select_from_pset()` method. This will load ALL parametes in the set in the same order that they are in the EMS parameter set. This method requires one input: the path to the parameter set, including the name of the set itself. Folders in the path need to be separated usuing a backslash ("\\"). To find the path to the parameter set, look at the "Load" menu in FDV+

```python
tsq.select_from_pset('folder1\folder2\set name')
```

Note: this method acts similarly to the `select()` method, so it will add to the existing list of selected parameters.
Note: this method will not search the EMS systems for parameters, so it will not add these to the local cache for later searching.

## Querying Analytics

You can retrieve a list of physical parameters for a flight by utilizing methods in the Analytic class. 

First, instantiate an analytic_query object with your connection (`c`) and a system id (`1`):

```python
analytic_query = Analytic(c, 1)
```

Then, call `analytic_query.get_physical_parameter_list(fr)` with a valid Flight Record:

```python
physicals = analytic_query.get_physical_parameter_list(flight_id = flight_id)
```

`physicals.sample(3)` looks like:

|     | id                                                                                                                                                                                                                                                                                                                                                                       | name                      | description                               | units   |
|----:|:-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|:--------------------------|:------------------------------------------|:--------|
| 277 | foobar123 | PARAMETER 1    | Uid: P1\nName: PARAMETER 1.    | DEG     |
| 696 | foobar124 | PARAMETER 2 | Uid: P2\nName: PARAMETER 2. | YR      |
|  48 | foobar125 | PARAMETER 3           | Uid: P3\nName: PARAMETER 3.           | DEG     |

You can also retrieve analytic metadata for a Flight (including for physical parameters):

```python
analytic_id = physicals['id'].iloc[0]
metadata = analytic_query.get_flight_analytic_metadata(analytic_id=analytic_id, flight_id=flight_id)
```

`metadata` looks like:

```python
{
    'Display\\Leading Zero': True,
    'Parameter\\Name': Foo
}
```

## Querying Parameter Sets (Analytic Sets)
You can retrieve information for parameter sets (called analytic sets in the API routes) using the `AnlyticSet` class.
Make sure you import the AnalyticSet class like this
```python
from emspy.query import AnalyticSet
```
Next, instantiate an analytic_set object using a connection (c) and a system_id (1)
```python
from emspy.query import AnalyticSet
analytic_set = AnalyticSet(c,1)
```
Finally use the `get_analytic_set()` method to get info for the set of interest. This method requires one input: the path to the parameter set, including the name of the set itself. Folders in the path need to be separated usuing a backslash ("\\"). To find the path to the parameter set, look at the "Load" menu in FDV+
```python
analytic_set_df = analytic_set.get_analytic_set('folder1\folder2\set name')
```

If you want to know what folders and sets are inside a folder, you can use the `get_group_content()` method. This takes just the path to the folder you are interested in as the only input.
To search the root folder, do not pass an input or use an empty string ("")
```python
analytic_set.get_group_content("folder1\folder2")
```
This method returns a dictionary with two keys: 'groups' and 'sets'. groups will have info about the folders, while sets will have info about parameter sets.