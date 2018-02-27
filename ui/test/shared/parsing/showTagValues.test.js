import showTagValuesParser from 'shared/parsing/showTagValues'

describe('showTagValuesParser', () => {
  it('handles an empty result set', () => {
    const response = {results: [{}]}

    const result = showTagValuesParser(response)

    expect(result.errors).toEqual([])
    expect(result.tags).toEqual({})
  })

  it('returns a an object of tag keys mapped to their values', () => {
    const response = {
      results: [
        {
          series: [
            {
              name: 'measurementA',
              columns: ['key', 'value'],
              values: [
                ['host', 'hostA'],
                ['host', 'hostB'],
                ['cpu', 'cpu0'],
                ['cpu', 'cpu1'],
              ],
            },
          ],
        },
      ],
    }

    const result = showTagValuesParser(response)

    expect(result.errors).toEqual([])
    expect(result.tags).toEqual({
      host: ['hostA', 'hostB'],
      cpu: ['cpu0', 'cpu1'],
    })
  })
})
