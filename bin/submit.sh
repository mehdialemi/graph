#!/bin/bash

SPARK_HOME="/home/$USER/Software/spark-1.6.1-bin-hadoop2.6"

jar_path="$PWD/bin/graph-processing.jar"
main_class=""
master="localhost"
exe_mem="1G"
total_cores=4

task=$1
case $task in
	"GCC_Deg") 	main_class="graph.clusteringco.FonlDegGCC"
	;;
	"LCC_Deg")	main_class="graph.clusteringco.FonlDegLCC"
	;;
	"GCC_Id")  main_class="graph.clusteringco.FonlIdGCC"
	;;
	"LCC_Id")  main_class="graph.clusteringco.FonlIdLCC"
    ;;
	*)	echo "please determine your task in the argument [GCC|LCC|GCC_Old|LCC_Old]"
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



