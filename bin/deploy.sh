#!/usr/bin/env bash

git pull --rebase origin master
mvn clean install
TARGET=
cp -ruf ./* $HOME/subgraph-mining/