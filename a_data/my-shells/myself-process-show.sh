nodes=$(cat /opt/my-shells/nodes)
# 在NODE01,NODE01,NODE03上启动了zookeeper
for zk in $nodes
do
 	echo "--------------------$zk---------------------"
	ssh root@$zk "jps"
	ssh root@$zk "netstat -tunlp | grep redis"
 	echo ""
done
