#!/usr/bin/env bash

INPUT_PREFIX="hdfs://graph-data"

typeset -A config
config=(
    [jar]="$PWD/target/subgraph-mining-1.0-jar-with-dependencies.jar"
    [master]="alemi-1"
    [cores]="10"
    [spark_home]="/home/$USER/spark-2.1"
    [inputs]="$PWD/bin/inputs"
)

if [ $# -lt 3 ]
then
    echo "No arguments supplied"
    echo "Usage: class_name config_file [other_arguments]"
    exit
fi

args=($@)
main_class=${args[0]}
conf_file=${args[1]}
class_name=`echo $main_class | rev | cut -d. -f1 | rev`
echo "main class: $main_class"
echo "config file: $conf_file"

other_args=""

if [ $# -gt 2 ]
then
    for i in $(seq 2 $#)
    do
        echo $i
        arg=${args[$i]}
        other_args="$other_args $arg"
    done
fi

# Read from config
while read -r line
do
    conf_name=`echo "$line" | cut -d= -f 1`
    config[$conf_name]=`echo "$line" | cut -d= -f 2`
    conf_value=${config[$conf_name]}
    echo "$conf_name = $conf_value"
done < $conf_file

input_file=${config[inputs]}

# Read input graphs
while read -r line
do
    graph_info="$line"
    info=($graph_info)
    graph_name=${info[0]}
    graph_path="$INPUT_PREFIX/$graph_name"
    partitionNum=${info[1]}

    # Create log dir
    log_dir="logs/$class_name/$graph_name"
    mkdir -p $logDir

    file_name=`date +%s`
    log_file="$log_dir/$file_name.log"
    touch $log_file
    echo "log file: $log_file"

    # Build argument
    jar_argument="$main_class $graph_path $partitionNum $other_args"
    echo "jar argument: $jar_argument"

    command="${config[spark_home]}/bin/spark-submit -class $main_class --total-executor-cores ${config[cores]} --master spark://${config[master]}:7077 ${config[jar]} $jar_argument"
    echo $command
    SECONDS=0

    $command > $log_file

done < $input_file