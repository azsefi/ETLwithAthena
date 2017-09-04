# ETLwithAthena
OLX recruitment tasks

Task1: There are some event data in json format in a S3 bucket. Extract DEVICE_ID, DATE and COUNTRY information from these json files.
Task2: Using extracted data calculate distinct device count per country

### Technology

For these tasks I mainly used AWS Athena. Because Athena is a service there is no need to set up infrastructure to use it. Therefore I preferred using Athena. 
Athena is AWS service using Presto as backend, which supports ANSI SQL. Pricing of Athena is based on size of processed data. Athena does not keep anything internally - reads from S3 bucket and writes to S3 bucket. Athena executes queries in parallel and scales automatically, so it is fast even with large datasets.
I used python/boto3 library to call Athena services.

### Solution

In Athena it is required to create table to query the data. Tables are actually defines structure of source data, and in contrary to traditional table concept, they do not keep any data itself. Thus, at first step a table (EVENTS) is created on the source JSON files. From name of the files it is obvious the source data is stored as parititioned (brand, tracker, channel, year, month, day). Therefore, I added partition to the table to read only the files which we are interested in. It will keep the data size smaller, which means lower cost for Athena in turn. At next step, EVENTS table queried to read the data. In Athena it is possible to set output path for result of the query. Thus, I put the resultset to the path based on the parameters (brand, mmonth, day etc.) we are running, to make it possible to create partitioned table on it for next steps. The file is created in output bucket with prefix "events/".
Before writing to a path, the path is always cleared to prevent duplicates in case of running the code more than once for any reason. 
After each query execution a function (waitexecution) checks the query status, and waits until the status changes from "RUNNING" to something else ("SUCCEDED", "FAILED").
At next step the extracted data (device_id, date, country) queried and aggregated to get number of distinct devices by country. Resultset is stored in the output bucket with prefix "DUA/".
To get more insight about customers I extracted more data from JSON files to do more analysis. Fields like 'device_id', 'event_time', 'country', 'longitude', 'latitude', 'user_id' extracted from source files, and stored in output bucket with prefix "event_details/". And, to do some visualization the event_details is read as pandas dataframe. It is the slowest part, because in this step it is required to read data from S3 into memory of local host.

````Python
>>> from eventloader import EventLoader
>>> from collections import OrderedDict
>>>
>>>
>>> access = 'xxxx'
>>> secret = 'yyyy'
````
I copied files under "s3://olx-bdt-recruit/de/horizontals/" to "s3://olxdata". My output bucket is "s3://shafi-outputs". Thus, "olxdata" is source bucket and "shafi-outputs" is output bucket.
```Python
>>> loader = EventLoader('olxdata', 'shafi-outputs', access, secret)
```
It is needed to define configuration parameter to control ETL. For example, same ETL may be required to run for IOS users instead of android users. So, config parameter should be modified accordingly. Please note that, the config values must be same as the "folder" structure of the source files. 

```Python
>>> config = OrderedDict()
>>> config['brand']   = 'olx'
>>> config['tracker'] = 'hydra'
>>> config['channel'] = 'android'
>>> config['year']    = '2017'
>>> config['month']   = '08'
>>> config['day']     = '09'
>>>
>>> loader.load(**config)
Tables initialized
Partition added to EVENTS table
Data extracted from HYDRA events:
        ExecutionTme:24.723 s DataScanned:1862.65 mb
EVENTS_FORMATTED table partitions refreshed
DAU data extracted:
        ExecutionTme:4.151 s DataScanned:465.64 mb
Partition added to EVENT_DETAILS table
Event datails extracted:
        ExecutionTme:39.3 s DataScanned:1862.63 mb
>>>
```
From output query execution time and the size of scanned data could be seen. 
EVENT_DETAILS should be read into a pandas dataframe to do some visualizations on it. Because data size can be very big it is recommended to put limit on the number of records to be read.

```Python
>>> loader.readEventDetails(7000000)
```

For task data prodvided for two countries: GH and ZA. To visualize how customers distributed through the country it is needed to provide country longitudes/latitudes (the postion of low-left, up-right corner of image). Morevover, to see the the points over the country it is required to send a dictionary parameter with longitude, latitude and label information.

```Python
>>> # (low-left long,lat), (up-right long, lat)
... za_borders = [(15,-35), (35,-20)]
>>>
>>> # Main cities
... za_cities={'longs' : [28.218370, 28.034088, 31.0292, 18.423300, 26.2249],
...            'lats'  : [-25.731340, -26.195246, -29.8579, -33.918861, -29.1183],
...            'labels': ['Pritoria', 'Johannesburg', 'Durban', 'Kapstadt', 'Bloemfontain']}

>>> loader.plot_country('za', za_borders, za_cities)
```
![Alt text](./images/za.jpg?raw=true "Optional Title")
