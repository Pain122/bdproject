#!/bin/bash

echo "Creating Hive partitioned and bucketed database"
hive -f ./sql/db.hql
