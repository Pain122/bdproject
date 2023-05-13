#!/bin/bash
echo "Cleaning the required directories"
rm -rf /project
echo "Removing the necessary directories from hdfs"
hdfs dfs -rm -r /project
echo "Unzipping the dataset!"
unzip -j -d ./data ./data/police.zip
