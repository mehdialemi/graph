#!/usr/bin/env bash

CONF_FILE="$PWD/bin/kcore/kcore.conf"

bin/run-spark.sh "ir.ac.sbu.graph.kcore.KCoreDegInfo" $CONF_FILE 4
bin/run-spark.sh "ir.ac.sbu.graph.kcore.KCoreNeighborList" $CONF_FILE 4
