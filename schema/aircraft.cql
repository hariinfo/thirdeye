create table thirdeye_test.airlineontime (
                year INT,
                quarter INT,
                month INT,
                dayofmonth INT,
                dayofweek INT,
                flight_date date,
                crsdeptime INT,	
                actualdeptime INT,	
                depdelayminutes INT,
                depdelay DECIMAL,
                crsarrtime INT,
                actualarrtime INT,
                arrdelayminutes INT,
                arrdelay DECIMAL,
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
                originstatename text,
                originstatecode text,
                destinationCityName text,
                destinationstatename text,
                destinationstatecode text,
                DepDel15 INT,	
                ArrDel15 INT,
                cancelled INT,
                CarrierDelay INT,
                WeatherDelay INT,
                NASDelay INT,
                SecurityDelay INT,
                LateAircraftDelay INT,
                close DECIMAL,
                ownership text,
                PRIMARY KEY ((flight_date), reporting_airline, crsdeptime, tail_number)
        )
        WITH comment='Airline on-time performance data';


create table thirdeye_test.airlineontime_delaytype (
                year INT,
                quarter INT,
                month INT,
                dayofmonth INT,
                dayofweek INT,
                flight_date date,
                crsdeptime INT,	
                actualdeptime INT,	
                depdelayminutes INT,
                depdelay DECIMAL,
                crsarrtime INT,
                actualarrtime INT,
                arrdelayminutes INT,
                arrdelay DECIMAL,
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
                originstatename text,
                originstatecode text,
                destinationCityName text,
                destinationstatename text,
                destinationstatecode text,
                DepDel15 INT,	
                ArrDel15 INT,
                cancelled INT,
                CarrierDelay INT,
                WeatherDelay INT,
                NASDelay INT,
                SecurityDelay INT,
                LateAircraftDelay INT,
                close DECIMAL,
                ownership text,
                PRIMARY KEY ((flight_date), ArrDel15)
        )
        WITH comment='Airline on-time performance data';
        
create table thirdeye_test.airlineontime_pbpp (
                year INT,
                quarter INT,
                month INT,
                dayofmonth INT,
                dayofweek INT,
                flight_date date,
                crsdeptime INT,	
                actualdeptime INT,	
                depdelayminutes INT,
                depdelay DECIMAL,
                crsarrtime INT,
                actualarrtime INT,
                arrdelayminutes INT,
                arrdelay DECIMAL,
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
                originstatename text,
                originstatecode text,
                destinationCityName text,
                destinationstatename text,
                destinationstatecode text,
                DepDel15 INT,	
                ArrDel15 INT,
                cancelled INT,
                CarrierDelay INT,
                WeatherDelay INT,
                NASDelay INT,
                SecurityDelay INT,
                LateAircraftDelay INT,
                close DECIMAL,
                ownership text,
                PRIMARY KEY ((flight_date, reporting_airline), ownership, ArrDel15, tail_number)
        )
        WITH comment='Airline on-time performance data';