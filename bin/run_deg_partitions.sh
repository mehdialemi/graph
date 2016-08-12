#!/bin/bash

run_command() {
    name=`echo $1 | cut -d ' ' -f 2`
        nohup $1 > $3/$name-$2.log 2>&1
}

#input="com-friendster.ungraph.txt"
#input="crawl2012.txt"
#input="twitter.txt"
#input="c2012*"
#input="orkut.txt"
input="c2014*"

if [ ! -d "logs" ]; then
    mkdir logs
fi
d=`date +%s`
logdir=logs/"$input-$d"
mkdir $logdir

p=1200
for i in {1..3}
do
	run_command "bin/submit.sh GCC_Deg $input $p" $i $logdir
        sleep 3

	run_command "bin/submit.sh LCC_Deg $input $p" $i $logdir
        sleep 3
	
	run_command "bin/submit.sh TC_Deg $input $p" $i $logdir
        sleep 3

        p=$(( p*2 ))
done
