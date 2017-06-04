#!/usr/bin/env bash

CONF_FILE="$PWD/bin/ktruss/ktruss.conf"

bin/run-spark.sh "ir.ac.sbu.graph.ktruss.spark.KTrussSparkTriangleVertices" $CONF_FILE 4
bin/run-spark.sh "ir.ac.sbu.graph.ktruss.spark.KTrussSpark" $CONF_FILE 4
bin/run-spark.sh "ir.ac.sbu.graph.ktruss.spark.KTrussInvalidUpdates" $CONF_FILE 4
bin/run-spark.sh "ir.ac.sbu.graph.ktruss.spark.KTrussSparkEdgeSup" $CONF_FILE 4
