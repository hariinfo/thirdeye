## 1. Data Cleansing
### 1.1 Dealing with NaN

### 1.2 Date format

## 2. Cassandra
### 2.1 SQL "NOT NULL" is not supported
Cassandra open source version has no support to perform a NOT null check in the SQL

The workaround in this case is to convert the null values to string literal such as 'NaN'. I used this technique for the Tail_Number field and replace all null value with 'NaN' during data cleansing task

### 2.2 SQL "WHERE" condition limitations
Where condition is only suported for columns that are defined either as a primary key or composite key

The workaround is to use ALLOW FILTERING which provides the capability to query the clustering columns using any condition.

The limitation of this workaround is that Datastax recommends this workaround only for development
## 3. Kafka
### 3.1 BufferError: Local: Queue full
 This error was thrown while streaming messages from python ETL to kafka topic posting messages. The transmission of data was failing at 50%.
The resolution to this issue was to tweat the connection configuration setting with the appropriate buffer size
Reference: https://github.com/edenhill/librdkafka/issues/117

## 4. Elasticsearch
