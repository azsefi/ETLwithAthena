# ETLwithAthena
OLX recruitment tasks

Task1: There are some event data in json format in a S3 bucket. Extract DEVICE_ID, DATE and COUNTRY information from these json files.
Task2: Using extracted data calculate distinct device count per country

### Technology

For these tasks I used aws Athena. Because Athena is a service there is no need to configure/install anything to use it. Therefore I preferred using Athena to get rid of setting up infrastructure. 
Athena is AWS service using Presto as backend, which supports ANSI SQL. Pricing of Athena is based on size of processed data. Athena does not keep anything internally - reads from S3 bucket and writes to S3 bucket. 
I used python/boto3 library to call Athena services.

### Solution

In Athena it is required to create table to query the data. Tables are actually defines structure of source data, and in contrary to traditional table concept, they do not keep any data itself. Thus, at step a table (EVENTS) is created on the source JSON files. From name of the files it is obvious the source data is stored as parititioned (brand, tracker, channel, year, month, day). Therefore, I added partition to the table to read only the files which we are interested in. It will keep the data size smaller, which means lower cost for Athena in turn. At next step, EVENTS table queried to read the data. In Athena it is possible to set output path for result of the query. Thus, I put the resultset to the path based on the parameters (brand, mmonth, day etc.) we are running, to make it possible to create partitioned table on it for next steps. Before writing to the path, the path is cleared to prevent duplicates in case of running the code more than once for any reason. 
