#!/usr/bin/env bash

git checkout -- target/subgraph-mining-1.0.jar
git pull --rebase origin master
mvn install
cp -r ./* ../../subgraph-mining/