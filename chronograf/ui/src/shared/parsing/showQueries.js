export default function parseshowQueries(response) {
  const results = response.results[0]
  if (results.error) {
    return {errors: [results.error], queries: []}
  }

  const series = results.series[0]
  if (!series.values) {
    return {errors: [], queries: []}
  }

  const queries = series.values.map(value => {
    return {
      id: value[series.columns.indexOf('qid')],
      database: value[series.columns.indexOf('database')],
      query: value[series.columns.indexOf('query')],
      duration: value[series.columns.indexOf('duration')],
    }
  })

  return {errors: [], queries}
}
