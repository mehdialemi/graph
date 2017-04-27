#!/usr/bin/env bash

CONF_FILE="$PWD/bin/kcore/sbm.conf"
CLASS_NAME="ir.ac.sbu.graph.kcore.KCoreNeighborList"

bin/run.sh $CLASS_NAME $CONF_FILE 4