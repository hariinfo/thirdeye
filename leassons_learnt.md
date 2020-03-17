## 1. Data Cleansing
### 1.1 Dealing with NaN

### 1.2 Date format

## 2. Cassandra
Cassandra database comes with a highly restrictive query model. My biggest learning from this assignment is Cassandra can not be thougt about as RDBMS. Cassandra CQL comes with just too many restrictions and is no where close to the RDBMS SQL.

### 2.1 Primary key definition
The PRIMARY KEY definition is made up of two parts: the Partition Key and the Clustering Columns. The first part maps to the storage engine row key, while the second is used to group columns in a row

### 2.2 Insert / Update operation
If primary is not defined properly we may see unexpected results while loading data into cassandra.
For instance at one point I noticed far fewer records as comapred the the source data and further investigation realized Cassandra automatically does an update of the existing record if it can not uniquely identify a row based on the primary key definition

### 2.3 No equivalent for SQL "IN" condition
I can not combine a filter operation and an IN condition

select * from thirdeye_test.airlineontime_byairline 
where  reporting_airline in ('DL','AA') and  flight_date >= '2017-01-01' and  flight_date < '2017-01-01' 
ALLOW FILTERING

Error: IN restrictions are not supported when the query involves filtering

Reference: https://stackoverflow.com/questions/26309198/cassandra-cql-or-operator

### 2.4 SQL "NOT NULL" is not supported
Cassandra open source version has no support to perform a NOT null check in the SQL.
This is becuase Cassandra is sparse, which means that only data that is used is actually stored.

The workaround in this case is to convert the null values to string literal such as 'NaN'. I used this technique for the Tail_Number field and replaced all null value with 'NaN' during data cleansing stage of ETL.

Reference: https://stackoverflow.com/questions/20981075/how-can-i-search-for-records-that-have-a-null-empty-field-using-cql

### 2.5 SQL "WHERE" condition limitations
Where condition is only suported for columns that are defined either as a primary key or composite key

The workaround is to use ALLOW FILTERING which provides the capability to query the clustering columns using any condition.

Error:
Cannot execute this query as it might involve data filtering and thus may have unpredictable performance. If you want to execute this query despite the performance unpredictability, use ALLOW FILTERING

The limitation of this workaround is that Datastax recommends this workaround only for development
## 3. Kafka
### 3.1 BufferError: Local: Queue full
 This error was thrown while streaming messages from python ETL to kafka topic posting messages. The transmission of data was failing at 50%.
The resolution to this issue was to tweat the connection configuration setting with the appropriate buffer size
Reference: https://github.com/edenhill/librdkafka/issues/117

## 4. Elasticsearch
