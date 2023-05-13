-- Dropping if it already exists
DROP DATABASE IF EXISTS projectdb;
-- Creating the database
CREATE DATABASE projectdb;
USE projectdb;
-- Setting the parameters just in case
SET mapreduce.map.output.compress = true;
SET mapreduce.map.output.compress.codec = org.apache.hadoop.io.compress.SnappyCodec;
-- Create incidents table
CREATE EXTERNAL TABLE incidents STORED AS AVRO LOCATION '/project/incidents' TBLPROPERTIES ('avro.schema.url'='/project/avsc/incidents.avsc');
-- Ping to check it is not dead
SELECT COUNT(*) FROM incidents;
-- It should output 727658

SET hive.exec.dynamic.partition = true;
SET hive.exec.dynamic.partition.mode = nonstrict;
SET hive.enforce.bucketing=true;

CREATE EXTERNAL TABLE incidents_part_buck(
    inc_datetime STRING NOT NULL,
    inc_date STRING NOT NULL,
    inc_time STRING NOT NULL,
    inc_weekday STRING NOT NULL,
    rep_datetime STRING NOT NULL,
    row_id BIGINT,
    inc_id INT NOT NULL,
    inc_number INT NOT NULL,
    cad_number FLOAT,
    rep_type_code STRING NOT NULL,
    rep_type_description STRING NOT NULL,
    filled_online BOOLEAN,
    inc_code INT NOT NULL,
    inc_category VARCHAR ( 44 ),
    inc_subcategory VARCHAR ( 40 ),
    inc_descr VARCHAR ( 84 ),
    resolution VARCHAR ( 20 ),
    intersection VARCHAR ( 84 ),
    cnn FLOAT,
    police_district VARCHAR ( 10 ),
    analysis_neigh VARCHAR ( 30 ),
    sup_distr FLOAT,
    sup_distr_2012 FLOAT,
    latitude FLOAT,
    longitude FLOAT,
    datapoint VARCHAR ( 46 ),
    neighbourhoods FLOAT,
    esncag FLOAT,
    cm_polygon FLOAT,
    cchr FLOAT,
    hsoc FLOAT,
    iin FLOAT,
    csd FLOAT,
    cpd FLOAT
) 
    PARTITIONED BY (inc_year int) 
    CLUSTERED BY (row_id) into 7 buckets
    STORED AS AVRO LOCATION '/project/incidents_part_buck' 
    TBLPROPERTIES ('AVRO.COMPRESS'='SNAPPY');

INSERT INTO incidents_part_buck partition (inc_year) 
SELECT inc_datetime, inc_date, inc_time, inc_year, inc_weekday, rep_datetime, row_id, inc_id, inc_number, cad_number, rep_type_code, rep_type_description, filled_online, inc_code, inc_category, inc_subcategory, inc_descr, resolution, intersection, cnn, police_district, analysis_neigh, sup_distr, sup_distr_2012, latitude, longitude, datapoint, neighbourhoods, esncag, cm_polygon, cchr, hsoc, iin, csd, cpd 
FROM incidents
DISTRIBUTE BY r;

INSERT INTO incidents_part_buck partition (inc_year) SELECT * FROM incidents;

    CREATE EXTERNAL TABLE incidents_part_buck( inc_datetime STRING, inc_date STRING, inc_time STRING, inc_weekday STRING, rep_datetime STRING, row_id BIGINT, inc_id INT, inc_number INT, cad_number FLOAT, rep_type_code STRING, rep_type_description STRING, filled_online BOOLEAN, inc_code INT, inc_category VARCHAR ( 44 ), inc_subcategory VARCHAR ( 40 ), inc_descr VARCHAR ( 84 ), resolution VARCHAR ( 20 ), intersection VARCHAR ( 84 ), cnn FLOAT, police_district VARCHAR ( 10 ), analysis_neigh VARCHAR ( 30 ), sup_distr FLOAT, sup_distr_2012 FLOAT, latitude FLOAT, longitude FLOAT, datapoint VARCHAR ( 46 ), neighbourhoods FLOAT, esncag FLOAT, cm_polygon FLOAT, cchr FLOAT, hsoc FLOAT, iin FLOAT, csd FLOAT, cpd FLOAT) PARTITIONED BY (inc_year int) CLUSTERED BY (row_id) into 7 buckets STORED AS AVRO LOCATION '/project/incidents_part_buck' TBLPROPERTIES ('AVRO.COMPRESS'='SNAPPY'); 