#!/bin/bash
rm -rf target
mkdir -p target/bin
cd src
CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o ../target/bin/mqdb
cp -rf conf ../target/
cd -
# cd target
# bin/mqdb -conf=conf/config.yml
