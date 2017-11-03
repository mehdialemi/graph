#!/usr/bin/env bash

CONF_FILE="$PWD/bin/ktruss/ktruss.conf"


#bin/run-spark.sh "ir.ac.sbu.graph.ktruss.spark.KTrussSpark" $CONF_FILE 4
bin/run-spark.sh "ir.ac.sbu.graph.spark.KTrussTSet2" $CONF_FILE 4 100
bin/run-spark.sh "ir.ac.sbu.graph.spark.KTrussTSet" $CONF_FILE 4 100
#bin/run-spark.sh "ir.ac.sbu.graph.ktruss.KTrussPregel" $CONF_FILE 4
#bin/run-spark.sh "ir.ac.sbu.graph.ktruss.spark.Cohen" $CONF_FILE 4
