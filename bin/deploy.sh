#!/usr/bin/env bash

git stash
git pull
#git pull --rebase origin master
mvn clean install
cp -ruf ./* $HOME/subgraph-mining/