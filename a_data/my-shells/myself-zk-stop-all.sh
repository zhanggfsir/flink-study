#!/bin/sh

for zk in NODE01 NODE02 NODE03
do
 ssh root@$zk "zkServer.sh stop"
done
