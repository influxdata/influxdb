export default function parseShowFieldKeys(response) {
  const errors = []
  const fieldSets = {}

  response.results.forEach(result => {
    if (result.error) {
      errors.push(result.error)
      return
    }

    if (!result.series) {
      return
    }

    const series = result.series[0]
    const fieldKeyIndex = series.columns.indexOf('fieldKey')
    const fields = series.values.map(value => {
      return value[fieldKeyIndex]
    })
    const measurement = series.name

    fieldSets[measurement] = fields
  })

  return {
    errors,
    fieldSets,
  }
}
