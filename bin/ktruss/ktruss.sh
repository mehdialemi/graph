#!/usr/bin/env bash

CONF_FILE="$PWD/bin/ktruss/ktruss.conf"

bin/run-spark.sh "ir.ac.sbu.graph.ktruss.spark.KTrussSparkTriangleSet" $CONF_FILE 4
bin/run-spark.sh "ir.ac.sbu.graph.ktruss.spark.KTrussSpark" $CONF_FILE 4
bin/run-spark.sh "ir.ac.sbu.graph.ktruss.KTrussPregel" $CONF_FILE 4
bin/run-spark.sh "ir.ac.sbu.graph.ktruss.spark.Cohen" $CONF_FILE 4
