## General path

The root path of etcd for influxdb clustering is "/influxdb", referenced as $INFLUXDB in the following sections.

In the following sections, the path is persitent path by default. If the path is ephemral, it will be marked as "Ephemeral" to yes expiclitly.

| Function | Path | Schema | Ephemeral | Note |
| -------- | ---- | ------ | --------- | ---- |
| Root | /influxdb | { "clusterId": unit64, "adminUserExists": bool } | | |
| Users | $INFLUXDB/users | | | |
| User | $INFLUXDB/users/<userName> | { "name": string, "password": "hashed string", "admin": bool, "previleges": {key string: value int} } | | |
| Databases | $INFLUXDB/dbs | | | |
| Database | $INFLUXDB/dbs/\<databaseName\> | { "name": string, "defaultRetentionPolicy": string } | | |
| Retention Policies | $INFLUXDB/rps | | | |
| Retention Policy | $INFLUXDB/rps/\<databaseName\>/\<retentionPolicyName\> | { "name": string, "replica": int, "duration": int64, "shardGroupDuration": int64 } | | |
| Shard Groups | $INFLUXDB/sgs | | | |
| Shard Group | $INFLUXDB/sgs/\<databaseName\>/\<retentionPolicyName\>/\<sgID\> | { "id": int64, "startTime": int64, "endTime": int64, "deleteAt": int64, "truncateAt": int64 } | | |
| Shards | $INFLUXDB/shards | | | |
| Shard | $INFLUXDB/shards/\<databaseName\>/\<retentionPolicyName\>/\<sgID\>/\<shardID\>/state | {"epoch": int64, "version": int, "replicas":[nodeId, ...]} | | |
| Continuous Queries | $INFLUXDB/cqs | | | |
| Coninuous Query | $INFLUXDB/cqs/\<databaseName\>/\<cqName\> | { "name": string, "query": string } | | |

## Data node

| Function | Path | Schema | Ephemeral | Note |
| -------- | ---- | ------ | --------- | ---- |
| Node root path | $INFLUXDB/nodes | | | |
| Node | $INFLUXDB/nodes/\<id\> | {"time": int64, "host":string, "version": int, "tcpHost": string } | yes | |

## Master node

| Function | Path | Schema | Ephemeral | Note |
| -------- | ---- | ------ | --------- | ---- |
| Naster path | $INFLUXDB/master | {"version": int,"nodeID": int, "time": int64} | yes | Used for leader election |
| Master epoch | $INFLUXDB/master_epoch | int64 | | Increase 1 for every round of elected leader |
