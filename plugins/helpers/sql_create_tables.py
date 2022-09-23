class SQLCreateTables:
    sql_create = ("""
        CREATE TABLE IF NOT EXISTS i94country
        (code int4 NOT NULL PRIMARY KEY,
        country varchar(100)
        );

        CREATE TABLE IF NOT EXISTS i94port
        (code varchar(5) NOT NULL PRIMARY KEY,
        port varchar(100)
        );

        CREATE TABLE IF NOT EXISTS i94mode
        (code int4 NOT NULL PRIMARY KEY,
        mode varchar(100)
        );

        CREATE TABLE IF NOT EXISTS i94address
        (code varchar(5) NOT NULL PRIMARY KEY,
        state varchar(100)
        );

        CREATE TABLE IF NOT EXISTS i94visa
        (code int4 NOT NULL PRIMARY KEY,
        type varchar(100)  
        );

        CREATE TABLE IF NOT EXISTS immigration(
        cicid int8 NOT NULL PRIMARY KEY,
        i94yr int4 NOT NULL,
        i94mon int4 NOT NULL,
        i94cit int4 REFERENCES i94country (code),
        i94res int4 REFERENCES i94country (code),
        i94port varchar(5) REFERENCES i94port (code),
        arrdate int8,
        i94mode int4 REFERENCES i94mode (code),
        i94addr varchar(5) REFERENCES i94address (code),
        depdate int8,
        i94bir int4,
        i94visa int4 REFERENCES i94visa (code),
        count int4,
        dtadfile int8,
        visapost varchar(5),
        occup varchar(5),
        entdepa char(1),
        entdepd char(1),
        entdepu char(1),
        matflag char(1),
        biryear int4,
        dtaddto varchar,
        gender char(1),
        insnum varchar,
        airline varchar(5),
        admnum varchar,
        fltno varchar(50),
        visatype varchar(5));


        CREATE TABLE IF NOT EXISTS demographics(
        city varchar(50),
        median_age float,
        male_population int8,
        female_population int8,
        total_population int8,
        no_of_veterans int8,
        foreign_born int8,
        avg_household_size float,
        state_code varchar(5) REFERENCES i94address (code),
        race varchar(100),
        count int8,
        PRIMARY KEY(state_code, city, race));


        CREATE TABLE IF NOT EXISTS airport_codes(
        ident varchar(10) not null PRIMARY KEY,      
        type varchar(100),
        name varchar,       
        elevation_ft float,
        continent varchar(50),   
        iso_country varchar(50),     
        iso_region varchar(50),    
        municipality varchar,   
        gps_code varchar(50),   
        iata_code varchar(50),       
        local_code varchar(50),
        coordinates varchar(50),
        latitude varchar(50),
        longitude varchar(50)); 


        CREATE TABLE IF NOT EXISTS global_temperature(
        date timestamp not null,
        avg_temperature float,
        avg_temperature_uncertainty float,
        city varchar(100),
        country varchar(100),
        latitude varchar(100),
        longitude varchar(100),
        PRIMARY KEY(date, latitude, longitude, city));
""")
    
























