# 1 Analytical Questions
Each analytical question has two sections.
A SQL query is used to run against the cassandra DB and a JSON query for Elasticsearch

## 1.1 Simple
### What is the min/max/average delays for an airline on a given day/month/year?  
```sql
select MAX(arrdelayminutes), MIN(arrdelayminutes), AVG(arrdelayminutes) from thirdeye_test.airlineontime
where year=2017 and month = 1 and dayofmonth=1 ALLOW FILTERING
```

```json
GET http://localhost:9200/my-index/_search
content-type: application/json

{
    "query": {
        "bool": {
        "must": [
            {
            "match": {
                "Year": 2017
            }
            },
            {
            "match": {
                "Month": 1
            }
            },
            {
            "match": {
                "DayofMonth": 1
            }
            }
        ]
        }
    },
  "aggs": {
        "avg_delay" : { "avg" : { "field" : "ArrDelayMinutes" } },
        "max_delay" : { "max" : { "field" : "ArrDelayMinutes" } },
        "min_delay" : { "min" : { "field" : "ArrDelayMinutes" } }
    }
}
```
### Top 3 Airlines that did not report the aircraft tail number (Aircraft tail number was null)

```sql
select * from thirdeye_test.airlineontime
where flight_date='2017-01-01' AND  tail_number = 'NaN' ALLOW FILTERING

```
```json
GET http://localhost:9200/my-index/_search
content-type: application/json

{
    "query": {
        "bool": {
        "must": [
            {
            "match": {
                "Tail_Number": "NaN"
            }
            }
        ]
        }
    },
     "size": 3
}
```

### Were there any specific airport with maximum delays on a given day?
```json
```
### What is the min/max and average time between delays by delay type on a given day?
```json
```
### Categories the number of delays by delay type
```json
```
### What is the min, max and average time between the planned and actual arrival time of the aircraft by airline?
```json
GET http://localhost:9200/my-index/_search
content-type: application/json

{
    "size": 0,
    "aggs" : {
        "group_by_name" : {
          "terms": {
            "field": "Reporting_Airline.keyword"
            ,"order" : {
              "arrival_delay": "desc"
            }
          },
          "aggs":{
            "arrival_delay":{
              "avg":{
                "field": "ArrDelayMinutes"
              }
            }
          }
        }    
    }
}
```

## 1.2 Moderate
## 1.3 Difficult
