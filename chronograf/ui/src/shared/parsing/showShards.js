import moment from 'moment'

export default function parseShowShards(results) {
  const shards = {}
  results.forEach(result => {
    if (!result.owners.length) {
      return
    }

    if (moment().isAfter(result['expire-time'])) {
      return
    }

    const shard = {
      shardId: result.id,
      database: result.database,
      retentionPolicy: result['retention-policy'],
      shardGroup: result['shard-group-id'],
      startTime: result['start-time'],
      endTime: result['end-time'],
      owners: result.owners,
    }

    const key = `${shard.database}..${shard.retentionPolicy}`
    if (shards[key]) {
      shards[key].push(shard)
    } else {
      shards[key] = [shard]
    }
  })

  return shards
}
