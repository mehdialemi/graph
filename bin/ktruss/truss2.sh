#!/usr/bin/env bash

CONF_FILE="$PWD/bin/ktruss/ktruss.conf"


bin/run-spark.sh "ir.ac.sbu.graph.spark.ktruss.KTrussTSet2" $CONF_FILE 120 1 1
#bin/run-spark.sh "ir.ac.sbu.graph.spark.ktruss.MaxKTrussTSetPartialUpdate" $CONF_FILE 120
