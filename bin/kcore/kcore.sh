#!/usr/bin/env bash

CONF_FILE="$PWD/bin/kcore/kcore.conf"

#bin/run-spark.sh "ir.ac.sbu.graph.kcore.KCoreDegInfo" $CONF_FILE 4
#for cores in {12..12}
#do
echo "youtube" > bin/inputs
for k in {4,40,51}
do
    bin/run-spark.sh "ir.ac.sbu.graph.spark.kcore.KCore" $CONF_FILE 120 $k 1000
done

echo "cit-patents" > bin/inputs
for k in {4,40,64}
do
    bin/run-spark.sh "ir.ac.sbu.graph.spark.kcore.KCore" $CONF_FILE 120 $k 1000
done

echo "soc-liveJournal" > bin/inputs
for k in {4,40,80,160,372}
do
    bin/run-spark.sh "ir.ac.sbu.graph.spark.kcore.KCore" $CONF_FILE 120 $k 1000
done

echo "orkut" > bin/inputs
for k in {4,40,80,160,253}
do
    bin/run-spark.sh "ir.ac.sbu.graph.spark.kcore.KCore" $CONF_FILE 120 $k 1000
done

echo "friendster" > bin/inputs
for k in {4,40,80,160,304}
do
    bin/run-spark.sh "ir.ac.sbu.graph.spark.kcore.KCore" $CONF_FILE 120 $k 1000
done

echo "twitter" > bin/inputs
for k in {4,40,80,160,2647}
do
    bin/run-spark.sh "ir.ac.sbu.graph.spark.kcore.KCore" $CONF_FILE 120 $k 1000
done



#done
