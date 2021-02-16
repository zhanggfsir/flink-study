# 停止kafka集群
nodes=$(cat /opt/my-shells/nodes)
for node in $nodes
do
	ssh root@$node "kafka-server-stop.sh"
done

sleep 15
# 在NODE01,NODE01,NODE03上停止zookeeper
for zk in $nodes
do
        #判断进程是否存在，如果不存在就启动它
        #PIDS=`ps -ef | grep QuorumPeerMain |grep -v grep | awk '{print $2}'`
        #if [ "$PIDS" != "" ]; then
                ssh root@$zk "zkServer.sh stop"
        #fi
done

