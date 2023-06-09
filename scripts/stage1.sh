#!/bin/bash
echo "Creating Postgres database!"
psql -U postgres -f ./sql/dataset.sql

sqoop import-all-tables \
    -Dmapreduce.job.user.classpath.first=true \
    --connect jdbc:postgresql://localhost/project \
    --username postgres \
    --warehouse-dir /project \
    --as-avrodatafile \
    --compression-codec=snappy \
    --outdir /project/avsc \
    --m 1


echo "Moving avsc files to hdfs"

hdfs dfs -mkdir /project
hdfs dfs -mkdir /project/avsc
hdfs dfs -put /project/avsc/incidents.avsc /project/avsc/
