#!/bin/bash
rm -rf target
mkdir -p target/bin
cd src
go build -o ../target/bin/mqdb
cp -rf conf ../target/
cd ../target
bin/mqdb -conf=conf/config.yml
