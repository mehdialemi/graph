#!/usr/bin/env bash

mvn clean -T 10 install -DskipTests -Dmaven.artifact.threads=20
