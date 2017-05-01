import _ from 'lodash'

// sums one or many queries for diskBytes from shards i.e.
// SELECT last(diskBytes) from "shards" 'GROUP BY *;
// SELECT last(diskBytes) from "shards" WHERE clusterID='123 'GROUP BY *;
// SELECT last(diskBytes) from "shards" WHERE nodeID='localhost:8088' AND clusterID='123 'GROUP BY *;
export function diskBytesFromShard(response) {
  const results = response.results[0]

  if (results.error) {
    return {errors: [results.error], bytes: 0}
  }

  if (!results.series) {
    return {errors: [], bytes: 0}
  }

  let sumBytes = 0
  response.results.forEach(result => {
    result.series.forEach(series => {
      const bytesIndex = series.columns.indexOf('last')
      sumBytes += series.values[0][bytesIndex]
    })
  })

  return {errors: [], bytes: sumBytes}
}

// Parses: SELECT last(diskBytes) from "shards" WHERE clusterID='localhost:8088' AND "database"='_internal' 'GROUP BY *;
export function diskBytesFromShardForDatabase(response) {
  const results = response.results[0]
  if (results.error) {
    return {errors: [results.error], shardData: {}}
  }

  if (!results.series) {
    return {errors: [], shardData: {}}
  }

  const shardData = results.series.reduce((data, series) => {
    const pathParts = series.tags.path.split('/')
    const shardID = pathParts[pathParts.length - 1]
    const diskUsage = series.values[0][series.columns.indexOf('last')]
    const nodeID = series.tags.nodeID

    // shardData looks like:
    //
    // {
    //    <shard ID>: [
    //      { nodeID: 'localhost:8088', diskUsage: 1241241 },
    //      { nodeID: 'localhost:8188', diskUsage: 1241241 },
    //    ],
    //    <shard ID>: ...
    // }
    //

    if (data[shardID]) {
      const index = _.findIndex(data[shardID], shard => shard.nodeID === nodeID)
      if (index > -1) {
        data[shardID][index].diskUsage += diskUsage
      } else {
        data[shardID].push({diskUsage, nodeID})
      }
    } else {
      data[shardID] = [{diskUsage, nodeID}]
    }

    return data
  }, {})

  return {errors: [], shardData}
}
