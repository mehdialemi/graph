#!/usr/bin/env bash

CONF_FILE="$PWD/bin/kcore/kcore.conf"

#bin/run-spark.sh "ir.ac.sbu.graph.kcore.KCoreDegInfo" $CONF_FILE 4
for k in {4,10,20,40,80,160}
do
    bin/run-spark.sh "ir.ac.sbu.graph.spark.KCore" $CONF_FILE $k 100
done
