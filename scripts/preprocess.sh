#!/bin/bash
echo "Cleaning the required directories"
rm -rf /project
echo "Unzipping teh dataset!"
unzip -j -d ./data ./data/police.zip