import parseShowTagKeys from 'shared/parsing/showTagKeys'

describe('parseShowTagKeys', () => {
  it('parses the tag keys', () => {
    const response = {
      results: [
        {
          series: [
            {name: 'cpu', columns: ['tagKey'], values: [['cpu'], ['host']]},
          ],
        },
      ],
    }

    const result = parseShowTagKeys(response)
    expect(result.errors).toEqual([])
    expect(result.tagKeys).toEqual(['cpu', 'host'])
  })

  it('handles empty results', () => {
    const response = {results: [{}]}

    const result = parseShowTagKeys(response)
    expect(result.errors).toEqual([])
    expect(result.tagKeys).toEqual([])
  })

  it('handles errors', () => {
    const response = {results: [{error: 'influxdb error'}]}

    const result = parseShowTagKeys(response)
    expect(result.errors).toEqual([response.results[0].error])
    expect(result.tagKeys).toEqual([])
  })
})
