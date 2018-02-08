#!/usr/bin/env bash

CONF_FILE="$PWD/bin/ktruss/ktruss.conf"


for cores in {12..12}
do
    for k in {4,10,20,40,80,160}
    do
        for kci in {0,1,2,3,4}
        do
            bin/run-spark.sh "ir.ac.sbu.graph.spark.ktruss.KTrussTSet" $CONF_FILE $cores $kci $k 100
        done
    done
done