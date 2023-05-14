#!/bin/bash
echo "Cleaning the required directories"
rm -rf /project
rm -rf ~/output
rm -rf ~/q1
rm -rf ~/q2
rm -rf ~/q3
rm -rf ~/q4
rm -rf ~/q5
echo "Removing the necessary directories from hdfs"
hdfs dfs -rm -r /project
echo "Unzipping the dataset!"
unzip -j -d ./data ./data/police.zip
echo "Installing requirements"
