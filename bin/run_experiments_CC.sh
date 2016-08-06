#!/bin/bash

run_command() {
    name=`echo $1 | cut -d ' ' -f 2`
	nohup $1 > logs/$name-$2.log 2>&1
}

#input="com-friendster.ungraph.txt"
input="com-amazon.ungraph.txt"

if [ ! -d "logs" ]; then
    mkdir logs
fi

d=`date +%s`
logdir=logs/"$input-$d"
mkdir $logdir

p=1200

IFS=',' read -ra TASKS <<< $1
for i in {1..3}; do
    for task in "${TASKS[@]}"; do
        SECONDS=0
        run_command "bin/submit.sh $task $input $p" $i  "$task-$logdir"
        echo "`LANG=de_DE date` Task=$task, Input=$input, Partitions=$p, Duration=$SECONDS, Log=$task-$logdir" >> logs/results.txt
        sleep 3
        p=$(( p*2 ))
	done
done
