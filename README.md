# ZKTimeMachine

A set of utilities for peering into the past of your zookeeper cluster

## Building

We use gradle for building. Gradle will download all dependencies. You don't even need to have gradle itself installed.

```
$ ./gradlew clean jar
```

This will generate a fat jar that contains all the dependencies in build/libs/zktimemachine.jar. This can be copied to the machines you want to run it on, or run in place.

## Running

There are two tools at present, TimeMachine and GrepOps.

TimeMachine allows you to dump the contents of a zookeeper server at any point in time in the past.

```
$ java -cp build/libs/zktimemachine.jar TimeMachine -z /var/lib/zookeeper/version-2/ -t 2015-03-31T00:58:56.176+02:00
Replaying up until time 2015-03-31T00:58:56.176+02:00
Applied 52112 transactions from zxid 8609234051 (Tue Mar 31 00:53:32 CEST 2015) to 8609286162 (Tue Mar 31 00:58:56 CEST 2015)
- / data:0 children:2 createdBy:0 createdAt:Thu Jan 01 01:00:00 CET 1970 age:1427756336176  
	- /midonet data:0 children:1 createdBy:0 createdAt:Tue May 20 15:29:57 CEST 2014 age:27163738878  
		- /midonet/v1 data:0 children:36 createdBy:0 createdAt:Tue May 20 15:29:57 CEST 2014 age:27163738816  
			- /midonet/v1/agents data:0 children:1 createdBy:0 createdAt:Tue May 20 15:29:57 CEST 2014 age:27163738554  
				- /midonet/v1/agents/ports data:0 children:0 createdBy:0 createdAt:Tue May 20 15:29:57 CEST 2014 age:27163738533  
			- /midonet/v1/vlan-bridges data:0 children:0 createdBy:0 createdAt:Tue May 20 15:29:57 CEST 2014 age:27163738775  
			- /midonet/v1/vniCounter data:5 children:0 createdBy:0 createdAt:Wed Sep 17 08:59:55 CEST 2014 age:16819140464  
			- /midonet/v1/pool_members data:0 children:0 createdBy:0 createdAt:Tue May 20 15:29:57 CEST 2014 age:27163738367  
			- /midonet/v1/port_groups data:0 children:0 createdBy:0 createdAt:Tue May 20 15:29:57 CEST 2014 age:27163738520  
			- /midonet/v1/load_balancers data:0 children:0 createdBy:0 createdAt:Tue May 20 15:29:57 CEST 2014 age:27163738384  
...
```

Grep Ops allows you to grep for all the ops that match a criteria, such as session id or zookeeper path.

```
$ java -cp build/libs/zktimemachine.jar GrepOps -z /var/lib/zookeeper/version-2/ --session 0x14cc6963c980010
2015-04-17T11:21:16.223+02:00, s:0x14cc6963c980010, zx:0x600006394 CREATE_SESSION(timeout=30000)
2015-04-17T11:21:16.827+02:00, s:0x14cc6963c980010, zx:0x6000063b0 MULTI (2 ops)
	-	DELETE(path=/midonet/v1/hosts/26272e43-e487-40df-abe3-df18354aee4a/vrnMappings/ports/24f40579-bb03-4f2b-8378-c41219a717ea)
	-	SET_DATA(path=/midonet/v1/ports/24f40579-bb03-4f2b-8378-c41219a717ea, data={"data":{"type":"BridgePort","properties":{},"device_id":"43bf0bc2-ab3f-4fda-af1e-100d2707e56d","adminStateUp":true,"inboundFilter":"790236f4-d5f2-4ad7-a7ff-c1d6147611aa","outboundFilter":"7c5e9c3b-16bf-44f3-a0d0-4f430e622590","portGroupIDs":null,"tunnelKey":47678,"hostId":null,"interfaceName":null,"peerId":null,"v1ApiType":null,"vlanId":null},"version":"1.8"}, version=3)

2015-04-17T11:21:48.000+02:00, s:0x14cc6963c980010, zx:0x600006a46 UNKNOWN(null)
```

Both tools will give you help if you ask them.

```
$ java -cp zktimemachine.jar GrepOps --help
  -e, --end-time  <arg>         Time to search to, in ISO8601 format
                                (default = 2015-05-07T13:09:49.461+02:00)
  -p, --path  <arg>             Filter by znode path
      --session  <arg>          Filter by session
  -s, --start-time  <arg>       Time to search from, in ISO8601 format
                                (default = 1970-01-01T00:00:00.000+00:00)
  -z, --zk-txn-log-dir  <arg>   ZooKeeper transaction log directory
      --help                    Show this message
```

This project is distributed under the Apache Software Foundation License, Version 2.
