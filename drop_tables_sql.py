drop_tables_sql = ("""
DROP TABLE IF EXISTS staging_airport_codes;
DROP TABLE IF EXISTS staging_demographics;
DROP TABLE IF EXISTS staging_global_temperature;
DROP TABLE IF EXISTS staging_i94address;
DROP TABLE IF EXISTS staging_i94country;
DROP TABLE IF EXISTS staging_i94mode;
DROP TABLE IF EXISTS staging_i94port;
DROP TABLE IF EXISTS staging_i94visa;
DROP TABLE IF EXISTS staging_immigration;

DROP TABLE IF EXISTS airport_codes;
DROP TABLE IF EXISTS demographics;
DROP TABLE IF EXISTS global_temperature;
DROP TABLE IF EXISTS i94address CASCADE;
DROP TABLE IF EXISTS i94country CASCADE;
DROP TABLE IF EXISTS i94mode CASCADE;
DROP TABLE IF EXISTS i94port CASCADE;
DROP TABLE IF EXISTS i94visa CASCADE;
DROP TABLE IF EXISTS immigration

""")
