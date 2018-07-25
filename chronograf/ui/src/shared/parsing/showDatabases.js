const parseShowDatabases = response => {
  const results = response.results[0]
  if (results.error) {
    return {errors: [results.error], databases: []}
  }

  const series = results.series[0]
  if (!series.values) {
    return {errors: [], databases: []}
  }

  const databases = series.values.map(s => {
    return s[0]
  })

  if (!databases.length) {
    alert('No databases were found.') // eslint-disable-line no-alert
  }

  return {errors: [], databases}
}

export default parseShowDatabases
