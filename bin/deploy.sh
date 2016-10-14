#!/usr/bin/env bash

git checkout .
git checkout -- target/subgraph-mining-1.0.jar
git checkout -- target/subgraph-mining-1.0-jar-with-dependencies.jar
git pull --rebase origin master
mvn clean install
cp -r ./* $HOME/subgraph-mining/