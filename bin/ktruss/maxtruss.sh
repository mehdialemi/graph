#!/usr/bin/env bash

CONF_FILE="$PWD/bin/ktruss/ktruss.conf"


bin/run-spark.sh "ir.ac.sbu.graph.spark.ktruss.MaxTrussTSetRange" $CONF_FILE 120 50 2
#bin/run-spark.sh "ir.ac.sbu.graph.spark.ktruss.MaxKTrussTSetPartialUpdate" $CONF_FILE 120
