export default function parseShowMeasurements(response) {
  const errors = []
  const measurementSets = []

  response.results.forEach((result, index) => {
    if (result.error) {
      errors.push({
        index,
        error: result.error,
      })
      return
    }

    if (!result.series) {
      measurementSets.push({
        index,
        measurements: [],
      })
      return
    }

    const series = result.series[0]
    const measurementNameIndex = series.columns.indexOf('name')
    const measurements = series.values.map(value => value[measurementNameIndex])

    measurementSets.push({
      index,
      measurements,
    })
  })

  return {
    errors,
    measurementSets,
  }
}
