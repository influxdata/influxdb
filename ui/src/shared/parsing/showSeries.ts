interface ParseShowSeriesResponse {
  errors: string[]
  series: string[]
}

const parseShowSeries = (response): ParseShowSeriesResponse => {
  const results = response.results[0]

  if (results.error) {
    return {errors: [results.error], series: []}
  }

  const series = results.series[0]

  if (!series.values) {
    return {errors: [], series: []}
  }

  const seriesValues = series.values.map(s => {
    return s[0]
  })

  return {errors: [], series: seriesValues}
}

export default parseShowSeries
