# emsPy
A Python Wrapper of EMS API. There is also a R wrapper for EMS API. If you are interest in the R version, please visit <https://github.build.ge.com/212401522/Rems>. The goal of this project is provide a way to bring EMS data in Python environment via the EMS's RESTful API. The project is still in an early alpha stage, so is not guarranteed working reliably nor well documented. I'll beef up the documentation as soon as possible.

Any contribution is welcome!

Dependency: numpy, pandas, networkx

## Installation
Right now, I've not yet packed this into an installation package. Just download or clone emsPy in a local directory and work on there. I'll make it as an installable package soon.

## Make an EMS API Connection

The optional proxy setting can be passed to the EMS connection object with the following format:
```python
proxies = {
    'http': 'http://{prxy_usrname}:{prxy_password}@{proxy_server_address}:{port},
    'https': 'https://{prxy_usrname}:{prxy_password}@{proxy_server_address}:{port}'
}
```

```python
from pems import Connection

c = Connection('kyungjin.moon',mysetting.pwd, proxies = proxies)

```

## Instantiate Query 


```python
from pems.query import Query

query = Query(c, ems_name = 'ems9')
```

Current limitations:
- The current query object only support the FDW Flight data source, which seems to be reasonable for POC functionality.
- Right now a Query object can be instantiated only with a single EMS system connection. I think a Query object with multi-EMS connection could be quite useful for data analysts who want to do study pseudo-global patterns.
    - Ex) query = Query(c, ems_name = ['ems9', 'ems10', 'ems11'])
- It does not support querying time-series raw parameters yet. Adding this capability may be the next major goal. 
 


## Write Query

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
query.select("P22: Fuel Burned by all Engines during Cruise", aggregate="avg")
```

### Datasource Setup

The EMS system handles with data fields based on a hierarchical tree structure. This field tree manages mappings between names and field IDs as well as the field groups of fields. In order to send query via EMS API, the Rems package will automatically generate a data file for the static, frequently used part of the field tree and load it as default. This bare field tree includes fields of the following field groups:

* Flight Information (sub-field groups Processing and Profile 16 Extra Data were excluded)
* Aircraft Information
* Flight Review
* Data Information
* Navigation Information
* Weather Information

In case that you want to query with fields that are not included in this default, stripped-down data tree, you'll have to add the field group where your fields belongs to and update your data field tree. For example, if you want to add a field group branch such as Profiles --> Standard Library Profiles --> Block-Cost Model --> P301: Block-Cost Model Planned Fuel Setup and Tests --> Measured Items --> Ground Operations (before takeoff), the execution of the following method will add the fields and their related subtree structure to the basic tree structure. You can use either the full name or just a fraction of consequtive keywords of each field group. The keyword is case insensitive.

**Caution**: the process of adding a subtree usually requires a very large number of recursive RESTful API calls which takes quite a long time. Please try to specify the subtree to as low level as possible to avoid a long processing time.

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

In case you want to save the data tree locally, specify your own choice of a file name with a proper path. For example, the following will save the data tree at your working directory.
```python
query.save_datatree('my_datatree.cpk')
```
If you saved the data tree in a local file, you will have to explicitly load the file to reuse the saved datatree.
```python
query.load_datatree('my_datatree.cpk')
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
- Each filtering condition is combined only by "AND" relationship
- The field keyword must be at left-hand side of a conditional expression
- No support of NULL value filtering, which is being worked on now
- The datetime condition should be only with the ISO8601 format

### ETC.
You can pass additional attributes supported by EMS query:


```python
# Returns only the distinct rows. Turned on as default
query.distinct(True)

# EMS allows max. 5000 of the rows for the output table. Default is 10. 
query.get_top(5000)
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
    

## Run Query and Retrieve Data
You can finally send the query to the EMS system and get the data. The output data is returned in Pandas' DataFrame object.



```python
df = query.run()

# This will return your data in Pandas dataframe format
```

