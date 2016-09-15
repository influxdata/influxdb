import AJAX from 'utils/ajax';
import {buildInfluxUrl, proxy} from 'utils/queryUrlGenerator';

export function showDatabases(host, clusterID) {
  const statement = `SHOW DATABASES`;
  const url = buildInfluxUrl({host, statement});

  return proxy(url, clusterID);
}

export function showQueries(host, db, clusterID) {
  const statement = 'SHOW QUERIES';
  const url = buildInfluxUrl({host, statement, database: db});

  return proxy(url, clusterID);
}

export function killQuery(host, queryId, clusterID) {
  const statement = `KILL QUERY ${queryId}`;
  const url = buildInfluxUrl({host, statement});

  return proxy(url, clusterID);
}

export function showMeasurements(host, database, clusterID) {
  const statement = 'SHOW MEASUREMENTS';
  const url = buildInfluxUrl({host, statement, database});

  return proxy(url, clusterID);
}

export function showRetentionPolicies(host, databases, clusterID) {
  let statement;
  if (Array.isArray(databases)) {
    statement = databases.map((db) => `SHOW RETENTION POLICIES ON "${db}"`).join('&q=');
  } else {
    statement = `SHOW RETENTION POLICIES ON "${databases}"`;
  }

  const url = buildInfluxUrl({host, statement});

  return proxy(url, clusterID);
}

export function showShards(clusterID) {
  return AJAX({
    url: `/api/int/v1/clusters/${clusterID}/show-shards`,
  });
}

export function createRetentionPolicy({host, database, rpName, duration, replicationFactor, clusterID}) {
  const statement = `CREATE RETENTION POLICY "${rpName}" ON "${database}" DURATION ${duration} REPLICATION ${replicationFactor}`;
  const url = buildInfluxUrl({host, statement});

  return proxy(url, clusterID);
}

export function dropShard(host, shard, clusterID) {
  const statement = `DROP SHARD ${shard.shardId}`;
  const url = buildInfluxUrl({host, statement});

  return proxy(url, clusterID);
}

export function showFieldKeys(host, database, measurement, clusterID) {
  const statement = `SHOW FIELD KEYS FROM "${measurement}"`;
  const url = buildInfluxUrl({host, statement, database});

  return proxy(url, clusterID);
}

export function showTagKeys(host, {database, retentionPolicy, measurement}, clusterID) {
  const statement = `SHOW TAG KEYS FROM "${measurement}"`;
  const url = buildInfluxUrl({host, statement, database, retentionPolicy});

  return proxy(url, clusterID);
}

export function showTagValues(host, {database, retentionPolicy, measurement, tagKeys, clusterID}) {
  const keys = tagKeys.sort().map((k) => `"${k}"`).join(', ');
  const statement = `SHOW TAG VALUES FROM "${measurement}" WITH KEY IN (${keys})`;

  const url = buildInfluxUrl({host, statement, database, retentionPolicy});

  return proxy(url, clusterID);
}
