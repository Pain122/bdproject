-- To run the script, copy it to the folder
-- /project
-- Then run:
-- psql -U postgres -f /project/dataset.sql
-- path to police.csv should be /project/police.csv
-- Dropping the database if exists
DROP DATABASE IF EXISTS project;
-- Creating the database
CREATE DATABASE project;
-- Switch to the database
\c project
-- add incidents table
CREATE TABLE incidents (
    inc_datetime char ( 22 ) NOT NULL,
    inc_date char ( 10 ) NOT NULL,
    inc_time char ( 5 ) NOT NULL,
    inc_year integer NOT NULL,
    inc_weekday VARCHAR ( 9 ) NOT NULL,
    rep_datetime char ( 22 ) NOT NULL,
    row_id bigint PRIMARY KEY,
    inc_id integer NOT NULL,
    inc_number integer NOT NULL,
    cad_number  float,
    rep_type_code CHAR ( 2 ) NOT NULL,
    rep_type_description VARCHAR ( 19 ) NOT NULL,
    filled_online BOOLEAN,
    inc_code integer NOT NULL,
    inc_category VARCHAR ( 44 ),
    inc_subcategory VARCHAR ( 40 ),
    inc_descr VARCHAR ( 84 ),
    resolution VARCHAR ( 20 ),
    intersection VARCHAR ( 84 ),
    cnn float,
    police_district VARCHAR ( 10 ),
    analysis_neigh VARCHAR ( 30 ),
    sup_distr float,
    "sup_distr_2012" float,
    latitude float,
    longitude float,
    "point" VARCHAR ( 46 ),
    neighbourhoods float,
    esncag float,
    cm_polygon float,
    cchr float,
    hsoc float,
    iin float,
    csd float,
    cpd float
);
-- Adding constraint about the weekdays
ALTER TABLE incidents
ADD CONSTRAINT correct_weekdays CHECK (INC_WEEKDAY in ('Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday',  'Sunday'));
-- Inserting values to the table
\COPY incidents FROM '/project/police.csv' DELIMITER ',' CSV HEADER;
-- Just a check to see how many rows were imported. It should be 727658
SELECT COUNT(*) FROM incidents;
