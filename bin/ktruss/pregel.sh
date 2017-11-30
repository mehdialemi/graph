#!/usr/bin/env bash

CONF_FILE="$PWD/bin/ktruss/ktruss.conf"

kci=0
k=4
bin/run-spark.sh "ir.ac.sbu.graph.ktruss.KTrussPregel" $CONF_FILE $k
