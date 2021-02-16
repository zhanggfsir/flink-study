# 关闭进程 
# 关闭es-sql-standalone插件
pkill -f node-server.js

# 关闭kibana插件
kill `ps -ef | grep node | grep -v grep | awk '{print $2}'`

# 关闭head插件
kill `ps -ef | grep grunt | grep -v grep | awk '{print $2}'`

# 关闭bigdesk插件
kill `ps -ef | grep SimpleHTTPServer | grep -v grep | awk '{print $2}'`

# 关闭es
for node in NODE01 NODE02 NODE03
do
 ssh root@$node "pkill -f Elasticsearch"
done
