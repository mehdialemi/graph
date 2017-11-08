#!/usr/bin/env bash

CONF_FILE="$PWD/bin/ktruss/ktruss.conf"


for k in {4,10,20,40,80,160}
do
    for kci in {0,1,2,3,4}
    do
        bin/run-spark.sh "ir.ac.sbu.graph.spark.KTrussTSet" $CONF_FILE $kci $k 100
    done
done