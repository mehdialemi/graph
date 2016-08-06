#!/bin/bash

run_command() {
    name=`echo $1 | cut -d ' ' -f 2`
	nohup $1 > logs/$name-$2.log 2>&1
}

#input="com-friendster.ungraph.txt"
#input="com-amazon.ungraph.txt"

input="twitter.txt"
minHobDeg=10000

if [ ! -d "logs" ]; then
    mkdir logs
fi

if [ ! -d "logs/old" ]; then
        mkdir logs/old
fi

d=`date +%s`

p=2400
IFS=',' read -ra TASKS <<< $1
for task in "${TASKS[@]}"; do
    logDir="logs/$task-$input"
    if [ -d $logDir ]; then
        mv $logDir logs/old/"$task-$input-$d"
    fi
    mkdir $logDir

    for i in {1..3}; do
        SECONDS=0
        run_command "bin/submit.sh $task $input $p $minHobDeg" $i  $logDir
        echo "`LANG=de_DE date` Task=$task, Input=$input, Partitions=$p, Duration=$SECONDS, Log=$logDir" >> logs/results.txt
        sleep 3
        p=$(( p*2 ))
    done
done