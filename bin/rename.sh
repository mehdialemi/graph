#!/bin/bash

files=($(ls $1))
prefix="$1"
for file in "${files[@]}"
do
  path="$prefix/$file"
  name=`grep KCore $path | cut -d ' ' -f 2`
  newp="$prefix/$name"
  echo "mv $path $newp"
  mv $path $newp
done

