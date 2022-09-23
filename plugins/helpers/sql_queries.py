class SqlQueries:
    airport_codes_insert = ("""
        TRUNCATE TABLE airport_codes;
        INSERT INTO airport_codes(
            SELECT
                ident,      
                type,
                name,       
                CAST(elevation_ft as float),
                continent,   
                iso_country,     
                iso_region,    
                municipality,   
                gps_code,   
                iata_code,       
                local_code,
                latitude,
                longitude FROM staging_airport_codes)
    """)

    demographics_insert = ("""
    TRUNCATE TABLE demographics CASCADE;
        INSERT INTO demographics
            (SELECT
                city,
                CAST(median_age as float),
                CAST(male_population as int8),
                CAST(female_population as int8),
                CAST(total_population as int8),
                CAST(no_of_veterans as int8),
                CAST(foreign_born as int8),
                CAST(avg_household_size as float),
                state_code,
                race,
                CAST(count as int8) FROM staging_demographics
                    where state_code IN (select code FROM staging_i94address))
    """)

    global_temperature_insert = ("""
        TRUNCATE TABLE global_temperature;
        INSERT INTO global_temperature
            (SELECT 
                CAST(date AS timestamp), 
                CAST(avg_temperature as FLOAT), 
                CAST(avg_temperature_uncertainty AS FLOAT),
                city, country, latitude, longitude
            FROM staging_global_temperature)
    """)

    i94address_insert = ("""
        TRUNCATE TABLE i94address CASCADE;
        INSERT INTO i94address
            (SELECT 
                CAST(code AS VARCHAR), 
                state FROM staging_i94address)
    """)

    i94country_insert = ("""
        TRUNCATE TABLE i94country CASCADE;
        INSERT INTO i94country
        (SELECT 
                CAST(code AS INT4), 
                country FROM staging_i94country)
    """)

    i94mode_insert = ("""
        TRUNCATE TABLE i94mode CASCADE;
        INSERT INTO i94mode
        (SELECT 
            CAST(code AS INT4), 
            mode FROM staging_i94mode)
    """)

    i94port_insert = ("""
        TRUNCATE TABLE i94port CASCADE;
        INSERT INTO i94port
        (SELECT 
            CAST(code AS VARCHAR), 
            port FROM staging_i94port)
    """)

    i94visa_insert = ("""
        TRUNCATE TABLE i94visa CASCADE;
        INSERT INTO i94visa
        (SELECT 
            CAST(code AS INT4), 
            type FROM staging_i94visa)
    """)

    immigration_insert = ("""
        TRUNCATE TABLE immigration CASCADE;
        INSERT INTO immigration(
            SELECT 
                CAST(cicid as int8),
                CAST(i94yr as int4),
                CAST(i94mon as int4),
                CAST(i94cit as int4),
                CAST(i94res as int4),
                i94port,
                CAST(arrdate as int8),
                CAST(i94mode as int4),
                i94addr,
                CAST(depdate as int8),
                CAST(i94bir as int4),
                CAST(i94visa as int4),
                CAST(count as int4),
                CAST(dtadfile as int8),
                visapost,
                occup,
                entdepa,
                entdepd,
                entdepu,
                matflag,
                CAST(biryear as int4),
                dtaddto,
                gender,
                insnum,
                airline,
                admnum,
                fltno,
                visatype 
            FROM staging_immigration 
                where i94addr IN (select code FROM staging_i94address )
                and CAST(i94cit as varchar) IN (SELECT CAST(code as varchar) FROM staging_i94country))
    """)