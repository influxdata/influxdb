import {getDeep} from 'src/utils/wrappers'

interface ParseShowSeriesResponse {
  errors: string[]
  series: string[]
}

const parseShowSeries = (response): ParseShowSeriesResponse => {
  const results = response.results[0]

  if (results.error) {
    return {errors: [results.error], series: []}
  }

  const seriesValues = getDeep<string[]>(results, 'series.0.values', [])

  if (!seriesValues.length) {
    return {errors: [], series: []}
  }

  const series = seriesValues.map(s => s[0])

  return {series, errors: []}
}

export default parseShowSeries
