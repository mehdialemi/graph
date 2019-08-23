#!/usr/bin/env bash

CONF_FILE="$PWD/bin/kcore/kcore.conf"

#bin/pattern-searcher.sh "ir.ac.sbu.graph.kcore.KCoreDegInfo" $CONF_FILE 4
bin/run-spark.sh "ir.ac.sbu.graph.spark.kcore.MaxCore" $CONF_FILE 120 $k 1000
