# 启动elasticsearch 
#su -c "~/es/bin/elasticsearch -d" jerry
for node in NODE01 NODE02 NODE03
do
 ssh root@$node "su -c '~/es/bin/elasticsearch -d' marry"
done

# 启动bigDesk插件
cd /opt/bigdesk/_site
nohup python -m SimpleHTTPServer >/dev/null 2>&1 &

# 启动head插件
cd  /opt/head/node_modules
nohup npm run start >/dev/null 2>&1 &

# 启动kibana插件 
cd /opt/kibana/bin
nohup ./kibana > /dev/null 2>&1 &

# 启动es-sql-site-standalone插件
cd /opt/es-standalone/site-server
nohup node node-server.js > /dev/null 2>&1 &
