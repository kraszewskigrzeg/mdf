# Micro Data Factory

## Description:

Easy to use, Postgres 13 based worflow for data collection, transformation and analysis.

## Create:

### 0) Always have your connection details:

```python 
import os

p = {
    'host': 'localhost',
    'port': '5432',
    'db_name': 'MicroDataFactory2',
    'user': 'postgres',
    'password': os.environ['PG_PASS']
}
```

### 1) Creating database

#### Create dabase schemas
You by running below code you will create database schemas and necessary functions to operate with MDF

```python 
from mdf2.manage import mdf_create, mdf_delete

mdf_create(p['host'],p['port'],p['db_name'],p['user'],p['password'])
```

Connect to your MDF:
```python 
from mdf2.mdf import MicroDataFactory

mdf = MicroDataFactory(p['host'],p['port'],p['db_name'],p['user'],p['password'])
```

To delete all of your data use:
```python 
mdf_delete(p['host'],p['port'],p['db_name'],p['user'],p['password'])
```

#### Idea of MDF

In MDF everything is grouped into Projects, DataFeeds and Datasets. 
Projects is the idea for you analysis to create new project use.

```python 
mdf.projects.create(name='polish_books',
                    description='description of polish books project', 
                    table_raw_config = {'column_names': [
                        'row_collection_date', 
                        'row_collection_date_time', 
                        'product_category', 
                        'product_id', 
                        'product_author', 
                        'product_title', 
                        'product_url', 
                        'product_price_current', 
                        'product_price_retail', 
                        'product_rating'],
                     'column_types': ['DATE','TIMESTAMP', 'TEXT','TEXT','TEXT','TEXT','TEXT','TEXT','TEXT','TEXT']},)
```

One `Project` is one table in database. Ofcourse you are able to mix data from different projects for your analysis.

To each `Project`, `DataFeeds` are assigned. `DataFeed` is a python program that will feed your table, in case more than one datafeed will be assigned to chosen `Project` remember that their data structure must match.  

```python 
mdf.datafeeds.create(4,
                     'EmpikBooks',
                     '/home/kragrz/PythonProjects/MicroDataFactory/PolishBooks/datafeeds/empik_books.py',
                     {'Aggregated-Overall-Count-Week-by-Week': {'type': 'Aggregated',
                                                                'column': 'product_id',
                                                                'function': 'count',
                                                                'groupby': ['category'],
                                                                'percent': 0.03,
                                                                'absolute': None,
                                                                'to_replace': '[\\s,]'}},)
```
There are some requirements for the `DataFeed` code:
1) Must be a class that inherites from `mdf.datafeeds.DataFeedBase`,

2) class name must be the same as `DataFeed` name,

3) Must implements method `execute` that returns `list` of `dicts` in the following format:

```python
[{'col1': 'val1', 'col2':'val3'}, 
 {'col1': 'val2', 'col2':'val4'}, 
 .
 .
 .
 {'col1': 'valn', 'col2':'valn'}
]
```
> number and names of columns must match the `table_raw_config` from `Project` you have created

Properly created `DataFeed` will produce `DataSets`

Each `DataSet` is equal to one execution of `DataFeed`

##### Scenario:

We want to analyse used cars prices in Poland.
Most popular website that provide listings is otomoto.pl

One time `DataFeed` execution can only provide us with snapshot of data. 
Lets collect data one time a week for a month (or more if you want to!).

At first we will create `Project`
Then we will collect the `DataSets` by designing and executing `DataFeed`.

When data will be collected we will try to analyze and visualize it.

## Quality and collection completeness:

MDF provides you with customizable data quality framework that can help you keep track on performance of your `DataFeeds`

## Enhancing the data:

MDF provides you with customizable data enhancemnet framework that can help you easly clean up your `DataSets`

## Manage:

## Analyze: 