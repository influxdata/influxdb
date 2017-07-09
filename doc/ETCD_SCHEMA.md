## General path

The root path of etcd for influxdb clustering is "/influxdb", referenced as $INFLUXDB in the following sections.

In the following sections, the path is persitent path by default. If the path is ephemral, it will be marked as "Ephemeral" to yes expiclitly.

| Function | Path | Schema | Ephemeral | Note |
| -------- | ---- | ------ | --------- | ---- |
| Root | /influxdb | { "clusterId": unit64, "adminUserExists": bool } | | |
| Users | $INFLUXDB/users | | | |
| User | $INFLUXDB/users/<userName> | { "name": string, "password": "hashed string", "admin": bool, "previleges": {key string: value int} } | | |
| Databases | $INFLUXDB/dbs | | | |
| Database | $INFLUXDB/dbs/\<databaseName\> | { "name": string, "defaultRetentionPolicy": string | | |
| Retention Policies | $INFLUXDB/dbs/\<databaseName\>/rps | | | |
| Retention Policy | $INFLUXDB/dbs/\<databaseName\>/rps/\<retentionPolicyName\> | { "name": string, "replica": int, "duration": int64, "shardGroupDuration": int64 } | | |
| Shard Groups | $INFLUXDB/dbs/\<databaseName\>/rps/\<retentionPolicyName\>/sgs | | | |
| Shard Group | $INFLUXDB/dbs/\<databaseName\>/rps/\<retentionPolicyName\>/sgs/\<sgID\> | { "id": int64, "startTime": int64, "endTime": int64, "deleteAt": int64, "truncateAt": int64 } | | |
| Shards | $INFLUXDB/dbs/\<databaseName\>/rps/\<retentionPolicyName\>/sgs/\<sgID\>/shards | | | |
| Shard | $INFLUXDB/dbs/\<databaseName\>/rps/\<retentionPolicyName\>/sgs/\<sgID\>/shards/\<shardID\>/state | {"epoch": int64, "version": int, "replicas":[nodeId, ...]} | | |
| Continuous Queries | $INFLUXDB/dbs/\<databaseName\>/cqs | | | |
| Coninuous Query | $INFLUXDB/dbs/\<databaseName\>/cqs/\<cqName\> | { "name": string, "query": string } | | |


## Data node

| Function | Path | Schema | Ephemeral | Note |
| -------- | ---- | ------ | --------- | ---- |
| Node root path | $INFLUXDB/nodes | | | |
| Node ids | $INFLUXDB/nodes/ids | | | |
| Node | $INFLUXDB/nodes/ids/\<id\> | {"time": int64, "host":string, "version": int, "tcpHost": string } | yes | |


## Master node

| Function | Path | Schema | Ephemeral | Note |
| -------- | ---- | ------ | --------- | ---- |
| Naster path | $INFLUXDB/master | {"version": int,"nodeID": int, "time": int64} | yes | |
| Master epoch | $INFLUXDB/master_epoch | int64 | | Increase 1 for every round of eleted leader |