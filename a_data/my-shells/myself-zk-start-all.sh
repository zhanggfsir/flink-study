#!/bin/sh

# 使用ssh启动Hadoop集群
# 步骤：
# step 1: 在NODE01,NODE01,NODE03上启动了zookeeper
for zk in NODE01 NODE02 NODE03
do
 ssh root@$zk "zkServer.sh start"
done

