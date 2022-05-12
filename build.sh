#!/bin/bash
rm -rf target
mkdir -p target/bin
mkdir -p target/conf
cd src
CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o ../target/bin/mqdb
cp -rf conf/config_template.yml ../target/conf/config.yml
cd -
# cd target
# bin/mqdb -conf=conf/config.yml
