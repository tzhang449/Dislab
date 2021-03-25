#!/usr/bin/bash
cd mrapps
make clean
make
cd ..
go run main/mrworker.go mrapps/wc.so