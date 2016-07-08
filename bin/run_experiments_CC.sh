#!/bin/bash
.submit.sh

for i in {1..5}
do
	bin/submit.sh GCC_Deg ~/graph-data/cit-Patents.txt 20
	bin/submit.sh GCC_Id ~/graph-data/cit-Patents.txt 20
	bin/submit.sh LCC_Deg ~/graph-data/cit-Patents.txt 20
	bin/submit.sh LCC_Id ~/graph-data/cit-Patents.txt 20
done 
