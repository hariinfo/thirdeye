DataStax cassandra driver
https://docs.datastax.com/en/developer/python-driver/3.21/
pip install cassandra-driver

run python tests
python3 -m tests.test_db

```sql
create table thirdeye_test.airlineontime (
                flight_date date,
                crsdeptime INT,	
                crsarrtime INT,	
                UNIQUE_CARRIER text,
                reporting_airline text,
                tail_number text,
                originairportid INT,	
                destairportid INT,
                aircraft_issue_date date, 
                carriername text,
                manufacturer text,
                aircraft_model text, 
                aircraft_type text, 
                aircraft_engine text,
                OriginCityName text,
                DepDel15 INT,	
                ArrDel15 INT,
                CarrierDelay INT,
                WeatherDelay INT,
                NASDelay INT,
                SecurityDelay INT,
                LateAircraftDelay INT,
                close DECIMAL,
                PRIMARY KEY (flight_date, tail_number, reporting_airline, crsdeptime)
        )
        WITH comment='Airline on-time performance data';
```
