# 1 Analytical Questions
## 1.1 Simple
### What is the min/max/average delays for an airline on a given day/month/year?  
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
    }
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
```

## 1.2 Moderate
## 1.3 Difficult
