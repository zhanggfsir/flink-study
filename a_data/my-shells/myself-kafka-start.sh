nodes=$(cat /opt/my-shells/nodes)
# 在NODE01,NODE01,NODE03上启动了zookeeper
for zk in $nodes
do
	ssh root@$zk "zkServer.sh start"
done

# 启动kafka集群
for node in $nodes
do
	ssh root@$node "kafka-server-start.sh -daemon /opt/kafka/config/server.properties"
done

