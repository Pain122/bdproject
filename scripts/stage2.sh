#!/bin/bash

echo "Creating Hive partitioned and bucketed database"
hive -f ./sql/db.hql
echo "Performing EDA"
hive -f ./sql/eda.hql
# Making directory
mkdir ~/output
# Query 1
echo "inc_category,num_incidents" > ~/output/q1.csv
cat /root/q1/* >> ~/output/q1.csv
# Query 2
echo "inc_year,num_incidents" > ~/output/q2.csv
cat /root/q2/* >> ~/output/q2.csv
# Query 3
echo "inc_weekday,num_incidents" > ~/output/q3.csv
cat /root/q3/* >> ~/output/q3.csv
# Query 4
echo "police_district,avg_response_time" > ~/output/q4.csv
cat /root/q4/* >> ~/output/q4.csv
# Query 5
echo "hour,num_incidents" > ~/output/q5.csv
cat /root/q5/* >> ~/output/q5.csv
echo "Done processing EDA"
