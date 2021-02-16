# 批量启动redis集群的脚本
i=1
for node in NODE01 NODE02 NODE03
do
 j=$[i+1]
 ssh root@$node "/opt/redis-cluster/redis0$i/src/redis-server /opt/redis-cluster/redis0$i/redis.conf"
 ssh root@$node "/opt/redis-cluster/redis0$j/src/redis-server /opt/redis-cluster/redis0$j/redis.conf"
 i=$[i+2]
done
