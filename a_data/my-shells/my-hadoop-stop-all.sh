#!/bin/sh

# 步骤：
# 在NODE02上停止AzkabanExecutorServer
#ssh root@NODE02 "cd /opt/azkaban-executor; nohup ./bin/azkaban-executor-shutdown.sh > /dev/null 2>&1 &"

# 在NODE03上停止AzkabanWebServer
#ssh root@NODE03 "cd /opt/azkaban-web; nohup ./bin/azkaban-web-shutdown.sh > /dev/null 2>&1 &"

# 停止gitblit服务
#ssh root@NODE01 "cd /opt/gitblit; gitblit-stop.sh"

#step 0: 停止hive服务
#ssh root@NODE01 "pkill -f metastore"
#ssh root@NODE01 "pkill -f hiveserver2"

# 使用ssh关闭Hadoop集群
# step 1: 在NODE01上关闭dfs
ssh root@NODE01 "stop-dfs.sh"


# step 2: 在NODE02上关闭yarn
ssh root@NODE02 "stop-yarn.sh"


# step 3: 在NODE03上关闭resourcemanager
ssh root@NODE03 "yarn-daemon.sh stop resourcemanager"

# step 4: 在NODE01,NODE01,NODE03上关闭了zookeeper
for zk in NODE01 NODE02 NODE03
do
 ssh root@$zk "zkServer.sh stop"
done
