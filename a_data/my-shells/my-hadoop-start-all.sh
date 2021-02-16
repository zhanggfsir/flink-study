#!/bin/sh

# 使用ssh启动Hadoop集群
# 步骤：
# step 1: 在NODE01,NODE01,NODE03上启动了zookeeper
for zk in NODE01 NODE02 NODE03
do
 ssh root@$zk "zkServer.sh start"
done

# step 2: 在NODE01上启动dfs
ssh root@NODE01 "start-dfs.sh"


# step 3: 在NODE02上启动yarn
ssh root@NODE02 "start-yarn.sh"


# step 4: 在NODE03上启动resourcemanager
ssh root@NODE03 "yarn-daemon.sh start resourcemanager"

# step 5: 启动hive服务
#ssh root@NODE01 "nohup hive --skiphbasecp --service metastore > /dev/null 2>&1 &"
#ssh root@NODE01 "nohup hive --skiphbasecp --service hiveserver2 > /dev/null 2>&1 &"

# step 6: 启动gitblit服务, nohup以后台进程的方式启动进程； /dev/null 将日志信息输出到黑洞中； 2：标准错误输出，1：控制台，&：结束
#ssh root@NODE01 "cd /opt/gitblit; nohup gitblit.sh > /dev/null 2>&1 &"

# step 7: 在NODE02上启动AzkabanExecutorServer
#ssh root@NODE02 "cd /opt/azkaban-executor; nohup ./bin/azkaban-executor-start.sh > /dev/null 2>&1 &"

# step 8: 在NODE03上启动AzkabanWebServer
#ssh root@NODE03 "cd /opt/azkaban-web; nohup ./bin/azkaban-web-start.sh > /dev/null 2>&1 &"

