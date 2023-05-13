-- Using db
USE projectdb;
-- Setting the parameters just in case
SET mapreduce.map.output.compress = true;
SET mapreduce.map.output.compress.codec = org.apache.hadoop.io.compress.SnappyCodec;
-- Find top 10 most occurring incidents
INSERT OVERWRITE LOCAL DIRECTORY '/root/q1'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
SELECT inc_category, COUNT(*) as count
FROM incidents
WHERE inc_category IS NOT NULL
GROUP BY inc_category
ORDER BY count DESC
LIMIT 10;
-- Find the number of incidents per year
INSERT OVERWRITE LOCAL DIRECTORY '/root/q2'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
SELECT inc_year, COUNT(*) as count
FROM incidents_part_buck
WHERE inc_year IS NOT NULL
GROUP BY inc_year
ORDER BY count DESC;
-- Count the average amount of incidents for each weekday
INSERT OVERWRITE LOCAL DIRECTORY '/root/q3'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
SELECT inc_weekday, AVG(cnt) as avg_incidents
FROM (
  SELECT inc_weekday, inc_date, COUNT(*) as cnt
  FROM incidents
  WHERE inc_date IS NOT NULL
  GROUP BY inc_weekday, inc_date
) t
GROUP BY inc_weekday;
-- Find average response time in minutes by police districts
INSERT OVERWRITE LOCAL DIRECTORY '/root/q4'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
SELECT police_district, AVG((unix_timestamp(rep_datetime, 'yyyy/MM/dd HH:mm:ss a') - unix_timestamp(inc_datetime, 'yyyy/MM/dd HH:mm:ss a')) / 60) as avg_response_time
FROM incidents
WHERE police_district IS NOT NULL AND inc_datetime IS NOT NULL AND rep_datetime IS NOT NULL
GROUP BY police_district;
-- Find average number of incidents per hour of day
INSERT OVERWRITE LOCAL DIRECTORY '/root/q5'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
SELECT SUBSTR(inc_time, 1, 2) as hour, COUNT(*) as num_incidents
FROM incidents
WHERE inc_time IS NOT NULL
GROUP BY SUBSTR(inc_time, 1, 2);
