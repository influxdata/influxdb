export default function parseShowTagKeys(response) {
  const results = response.results[0]
  if (results.error) {
    return {errors: [results.error], tagKeys: []}
  }

  if (!results.series) {
    return {errors: [], tagKeys: []}
  }

  const series = results.series[0]
  if (!series.values) {
    return {errors: [], tagKeys: []}
  }

  const tagKeys = series.values.map(v => {
    return v[0]
  })

  return {errors: [], tagKeys}
}
