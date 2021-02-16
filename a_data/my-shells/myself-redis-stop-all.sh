# 批量停止redis集群的脚本
i=1
port=7001
for node in NODE01 NODE02 NODE03
do
 j=$[i+1]
 portNext=$[port+1]
 ssh root@$node "/opt/redis-cluster/redis0$i/src/redis-cli -h $node -p $port -a 123  shutdown"
 ssh root@$node "/opt/redis-cluster/redis0$j/src/redis-cli -h $node -p $portNext -a 123 shutdown"
 i=$[i+2]
 port=$[port+2]
done
