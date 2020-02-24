Had to remove records with no aircraft Tailnumber details

```sql
 create table thirdeye_test.airlineontime (
        FL_DATE timestamp,
        UNIQUE_CARRIER text,
        FL_NUM int,
        ORIGIN_AIRPORT_ID int,
        ORIGIN_AIRPORT_SEQ_ID int,
        ORIGIN_CITY_MARKET_ID int,
        DEST_AIRPORT_ID int,
        DEST_AIRPORT_SEQ_ID int,
        DEST_CITY_MARKET_ID int,
        CRS_DEP_TIME int,
        DEP_TIME int,
        DEP_DELAY double,
        CRS_ARR_TIME int,
        ARR_TIME int,
        ARR_DELAY double,
        CRS_ELAPSED_TIME double,
        ACTUAL_ELAPSED_TIME double,
        PRIMARY KEY (FL_DATE, UNIQUE_CARRIER, FL_NUM, CRS_DEP_TIME)
        )
        WITH comment='Airline on-time performance data';
```

```sql
create table thirdeye_test.airlineontime (
        FL_DATE timestamp,
        UNIQUE_CARRIER text,
        reporting_airline text,
        tail_number text,
		originairportid INT,	
		destairportid INT,
        PRIMARY KEY (reporting_airline)
        )
        WITH comment='Airline on-time performance data';
```