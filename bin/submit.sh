#!/bin/bash

SPARK_HOME="/home/$USER/Software/spark-1.6.2-bin-hadoop2.6"

jar_path="$PWD/bin/graph-processing.jar"
main_class=""
master="malemi-2"
exe_mem="10G"
total_cores=20

task=$1
case $task in
	"GCC_Deg") 	main_class="graph.clusteringco.FonlDegGCC"
	;;
	"LCC_Deg")	main_class="graph.clusteringco.FonlDegLCC"
	;;
	"TC_Deg")   main_class="graph.clusteringco.FonlDegTC"
	;;
	"GCC_Id")  main_class="graph.clusteringco.FonlIdGCC"
	;;
	"LCC_Id")  main_class="graph.clusteringco.FonlIdLCC"
	;;
	"TC_Id")    main_class="graph.clusteringco.FonlIdTC"
	;;
	"GCC_GraphX") main_class="graph.clusteringco.GraphX_GCC"
	;;
	"LCC_GraphX") main_class="graph.clusteringco.GraphX_LCC"
	;;
	"TC_GraphX") main_class="graph.clusteringco.GraphX_TC"
	;;
	"GCC_NodeIter") main_class="graph.clusteringco.NodeIteratorPlusGCC_Spark"
	;;
	"TC_NodeIter")  main_class="graph.clusteringco.NodeIteratorPlusTC_Spark"
	;;
	*)	echo "please determine your task in the argument [GCC|LCC|GCC_Old|LCC_Old]"
		exit 1
esac

dataset="$HOME/$2"
p="$3"

if [ "$master" != "localhost" ]; then
	dataset="hdfs://$master/graph-data/$2"
fi

cd $SPARK_HOME

echo "Running $main_class on $master with partitions $p"
bin/spark-submit --class $main_class --executor-memory $exe_mem --total-executor-cores $total_cores --master spark://$master:7077 $jar_path $dataset $p



