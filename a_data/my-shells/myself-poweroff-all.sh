for node in NODE03 NODE02 NODE01
do
	ssh root@$node "poweroff"
done
