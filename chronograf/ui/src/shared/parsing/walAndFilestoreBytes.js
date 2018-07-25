export default function walAndFilestoreBytes(response) {
  let totalDiskBytes = 0

  response.results.forEach(result => {
    if (!result.series) {
      return
    }
    result.series.forEach(series => {
      if (!series.values) {
        return
      }
      series.values.forEach(value => {
        totalDiskBytes += (value && value[1]) || 0
      })
    })
  })

  return totalDiskBytes
}
