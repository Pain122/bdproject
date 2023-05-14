#!/bin/bash
# echo "Submitting the model"
# spark-submit --jars /usr/hdp/current/hive-client/lib/hive-metastore-1.2.1000.2.6.5.0-292.jar,/usr/hdp/current/hive-client/lib/hive-exec-1.2.1000.2.6.5.0-292.jar --packages org.apache.spark:spark-avro_2.12:3.0.3 ./models/model.py

echo "moving csvs"
cp /project/output/lr_lon_predictions.csv/*.csv ~/output
for f in ~/output/part*.csv; do mv "$f" ~/output/lr_lon_predictions.csv; done

cp /project/output/lr_lat_predictions.csv/*.csv ~/output
for f in ~/output/part*.csv; do mv "$f" ~/output/lr_lat_predictions.csv; done

cp /project/output/gbt_lat_predictions.csv/*.csv ~/output
for f in ~/output/part*.csv; do mv "$f" ~/output/gbt_lat_predictions.csv; done

cp /project/output/gbt_lon_predictions.csv/*.csv ~/output
for f in ~/output/part*.csv; do mv "$f" ~/output/gbt_lon_predictions.csv; done

rm ./output/*
cp ~/output/* ./output
