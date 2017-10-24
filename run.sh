#!/bin/bash

rm -rf ./sense
rm -rf ./overall_seasonal
rm -rf ./txn_cat_overall
rm -rf ./txn_cat_seasonal

spark-submit --driver-memory 48g PIGEON_SWIFT.py $1 $2

awk 'FNR==1 && NR!=1{next;}{print}' sense/* > ./output/sense.csv
awk 'FNR==1 && NR!=1{next;}{print}' overall_seasonal/* > ./output/overall_seasonal.csv
awk 'FNR==1 && NR!=1{next;}{print}' txn_cat_overall/* > ./output/txn_cat_overall.csv
awk 'FNR==1 && NR!=1{next;}{print}' txn_cat_seasonal/* > ./output/txn_cat_seasonal.csv

rm -rf ./sense
rm -rf ./overall_seasonal
rm -rf ./txn_cat_overall
rm -rf ./txn_cat_seasonal
