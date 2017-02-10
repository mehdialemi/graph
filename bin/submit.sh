#!/bin/bash

if [ -z ${SPARK_HOME+X} ]; then
    SPARK_HOME="/home/$USER/spark-2"
fi

jar_path="$PWD/target/subgraph-mining-1.0-jar-with-dependencies.jar"
master="alemi-1"

if [ ! -z ${TOTAL_CORES+X} ]; then
    total_cores=$TOTAL_CORES
else
    total_cores=100
fi

persistOnDisk="false"
repartition="true"
userSort="true"

task=$1
case $task in
    "GCC_Hob")  main_class="ir.ac.sbu.graph.clusteringco.FonlHobGCC"
    ;;
	"GCC_Deg") 	main_class="ir.ac.sbu.graph.clusteringco.FonlDegGCC"
	;;
	"LCC_Deg")	main_class="ir.ac.sbu.graph.clusteringco.FonlDegLCC"
	;;
	"TC_Deg")   main_class="ir.ac.sbu.graph.clusteringco.FonlDegTC"
	;;
	"GCC_Id")  main_class="ir.ac.sbu.graph.clusteringco.FonlIdGCC"
	;;
	"LCC_Id")  main_class="ir.ac.sbu.graph.clusteringco.FonlIdLCC"
	;;
	"TC_Id")    main_class="ir.ac.sbu.graph.clusteringco.FonlIdTC"
	;;
	"GCC_GraphX") main_class="ir.ac.sbu.graph.clusteringco.GraphX_GCC"
	;;
	"LCC_GraphX") main_class="ir.ac.sbu.graph.clusteringco.GraphX_LCC"
	;;
	"TC_GraphX") main_class="ir.ac.sbu.graph.clusteringco.GraphX_TC"
	;;
	"GCC_NodeIter") main_class="ir.ac.sbu.graph.clusteringco.NodeIteratorPlusGCC_Spark"
	;;
	"TC_NodeIter")  main_class="ir.ac.sbu.graph.clusteringco.NodeIteratorPlusTC_Spark"
	;;
	"TC_Cohen") main_class="ir.ac.sbu.graph.clusteringco.CohenTC"
	;;
	"TC_Pregel") main_class="ir.ac.sbu.graph.clusteringco.PregelTC"
	;;
	"Dist") main_class="ir.ac.sbu.graph.stat.GraphStat"
	;;
	"Dist_Redis") main_class="ir.ac.sbu.graph.utils.GraphStatRedis"
	;;
	"KT_RT") main_class="ir.ac.sbu.graph.ktruss.distributed.RebuildTriangles"
	;;
	"KT_EVL") main_class="ir.ac.sbu.graph.ktruss.distributed.EdgeVertexList"
	;;
	"KT_EVL_ITER") main_class="ir.ac.sbu.graph.ktruss.distributed.EdgeNodesIterByThreshold"
	;;
	"KT_EVL_ITER2") main_class="ir.ac.sbu.graph.ktruss.distributed.KTrussSparkIntervalByConstant"
	;;
	"KT_Cohen") main_class="ir.ac.sbu.graph.ktruss.distributed.Cohen"
	;;
	"KT_Pregel") main_class="ir.ac.sbu.graph.ktruss.KTrussPregel"
	;;
	"KT_SPARK") main_class="ir.ac.sbu.graph.ktruss.distributed.KTrussSparkEdgeVertices"
	;;
	"KT_TRIANGLE") main_class="ir.ac.sbu.graph.ktruss.distributed.KTrussSparkTriangle"
	;;
	"KT_TRIANGLE_COMP") main_class="ir.ac.sbu.graph.ktruss.distributed.KTrussSparkTriangleSet"
	;;
	"KT_INV_VERTICES") main_class="ir.ac.sbu.graph.ktruss.distributed.KTrussSparkInvalidVertices"
	;;
	*)	echo "please determine your task in the argument
	[GCC_Hob|GCC_Deg|LCC_Deg|TC_Deg|GCC_Id|LCC_Id|GCC_GraphX|LCC_GraphX|TC_GraphX|GCC_NodeIter|TC_NodeIter"
		exit 1
esac

dataset="$HOME/$2"
p="$3"

if [ "$master" != "localhost" ]; then
	dataset="hdfs://$master/graph-data/$2"
fi

cd $SPARK_HOME

echo "Running $main_class on $master with partitions $p"
bin/spark-submit --class $main_class --total-executor-cores $total_cores --master spark://$master:7077 $jar_path $dataset $p $4 $5 $persistOnDisk $repartition $userSort
