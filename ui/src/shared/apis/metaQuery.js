import AJAX from 'utils/ajax'
import _ from 'lodash'
import {buildInfluxUrl, proxy} from 'utils/queryUrlGenerator'

export const showDatabases = async source => {
  const query = 'SHOW DATABASES'
  return await proxy({source, query})
}

export const showRetentionPolicies = async (source, databases) => {
  let query
  if (Array.isArray(databases)) {
    query = databases.map(db => `SHOW RETENTION POLICIES ON "${db}"`).join(';')
  } else {
    query = `SHOW RETENTION POLICIES ON "${databases}"`
  }

  return await proxy({source, query})
}

export function showQueries(source, db) {
  const query = 'SHOW QUERIES'

  return proxy({source, query, db})
}

export function killQuery(source, queryID) {
  const query = `KILL QUERY ${queryID}`

  return proxy({source, query})
}

export const showMeasurements = async (source, db) => {
  const query = 'SHOW MEASUREMENTS'

  return await proxy({source, db, query})
}

export const showTagKeys = async ({
  source,
  database,
  retentionPolicy,
  measurement,
}) => {
  const rp = _.toString(retentionPolicy)
  const query = `SHOW TAG KEYS FROM "${rp}"."${measurement}"`
  return await proxy({source, db: database, rp: retentionPolicy, query})
}

export const showTagValues = async ({
  source,
  database,
  retentionPolicy,
  measurement,
  tagKeys,
}) => {
  const keys = tagKeys.sort().map(k => `"${k}"`).join(', ')
  const rp = _.toString(retentionPolicy)
  const query = `SHOW TAG VALUES FROM "${rp}"."${measurement}" WITH KEY IN (${keys})`

  return await proxy({source, db: database, rp: retentionPolicy, query})
}

export function showShards() {
  return AJAX({
    url: '/api/int/v1/show-shards',
  })
}

export function createRetentionPolicy({
  host,
  database,
  rpName,
  duration,
  replicationFactor,
  clusterID,
}) {
  const statement = `CREATE RETENTION POLICY "${rpName}" ON "${database}" DURATION ${duration} REPLICATION ${replicationFactor}`
  const url = buildInfluxUrl({host, statement})

  return proxy(url, clusterID)
}

export function dropShard(host, shard, clusterID) {
  const statement = `DROP SHARD ${shard.shardId}`
  const url = buildInfluxUrl({host, statement})

  return proxy(url, clusterID)
}

export const showFieldKeys = async (source, db, measurement, rp) => {
  const query = `SHOW FIELD KEYS FROM "${rp}"."${measurement}"`

  return await proxy({source, query, db})
}
