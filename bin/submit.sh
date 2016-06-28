#!/bin/bash

SPARK_HOME="/home/$USER/Software/spark-1.6.1-bin-hadoop2.6"

jar_path="$PWD/bin/graph-processing.jar"
main_class=""
master="localhost"
exe_mem="1G"
total_cores=4

task=$1
case $task in
	"GCC") 	main_class="graph.clusteringco.GlobalCC"
	;;
	"LCC")	main_class="graph.clusteringco.LocalCC"
	;;
	*)	echo "please determine your task in the argument [GCC|LCC]"
		exit 1
esac

dataset="$2"
p="$3"

if [ "$master" != "localhost" ]; then
	dataset="hdfs://$master/graph-data/$2"
fi

cd $SPARK_HOME

echo "Running $main_class on $master with partitions $p"
bin/spark-submit --class $main_class --executor-memory $exe_mem --total-executor-cores $total_cores --master spark://$master:7077 $jar_path $dataset $p



