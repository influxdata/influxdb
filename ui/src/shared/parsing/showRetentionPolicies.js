export default function parseShowRetentionPolicies(result) {
  if (result.error) {
    return {errors: [result.error], retentionPolicies: []}
  }

  const series = result.series[0]
  if (!series.values) {
    return {errors: [], retentionPolicies: []}
  }

  const columns = series.columns

  const nameIndex = columns.indexOf('name')
  const durationIndex = columns.indexOf('duration')
  const shardGroupDurationIndex = columns.indexOf('shardGroupDuration')
  const replicationIndex = columns.indexOf('replicaN')
  const defaultIndex = columns.indexOf('default')

  const retentionPolicies = series.values.map(arr => {
    return {
      name: arr[nameIndex],
      duration: arr[durationIndex],
      shardGroupDuration: arr[shardGroupDurationIndex],
      replication: arr[replicationIndex],
      isDefault: arr[defaultIndex],
    }
  })

  return {errors: [], retentionPolicies}
}
