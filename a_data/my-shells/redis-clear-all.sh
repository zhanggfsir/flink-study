# 批量删除redis集群持久化文件以及配置文件的脚本
for node in NODE01 NODE02 NODE03
do
 ssh root@$node "rm -rf /root/dump.rdb"
 ssh root@$node "rm -rf /root/appendonly.aof"
 ssh root@$node "rm -rf /root/nodes-*.conf"
done

# 批量停止redis集群的脚本
i=1
port=7001
for node in NODE01 NODE02 NODE03
do
	 j=$[i+1]
	 portNext=$[port+1]
	 ssh root@$node "/opt/redis-cluster/redis0$i/src/redis-cli -h $node -p $port CLUSTER RESET"
	 ssh root@$node "/opt/redis-cluster/redis0$i/src/redis-cli -h $node -p $port FLUSHALL"
	 ssh root@$node "/opt/redis-cluster/redis0$j/src/redis-cli -h $node -p $portNext CLUSTER RESET"
	 ssh root@$node "/opt/redis-cluster/redis0$j/src/redis-cli -h $node -p $portNext FLUSHALL"
	 i=$[i+2]
	 port=$[port+2]
done
