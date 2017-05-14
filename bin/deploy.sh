#!/usr/bin/env bash

git pull --rebase origin master
mvn clean install
rm -r $HOME/subgraph-mining
cp -r ./* $HOME/subgraph-mining/