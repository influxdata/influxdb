import parseShowFieldKeys from 'shared/parsing/showFieldKeys'

describe('parseShowFieldKeys', () => {
  it('parses a single result', () => {
    const response = {
      results: [
        {
          series: [
            {name: 'm1', columns: ['fieldKey'], values: [['f1'], ['f2']]},
          ],
        },
      ],
    }

    const result = parseShowFieldKeys(response)
    expect(result.errors).toEqual([])
    expect(result.fieldSets).toEqual({
      m1: ['f1', 'f2'],
    })
  })

  it('parses multiple results', () => {
    const response = {
      results: [
        {
          series: [
            {name: 'm1', columns: ['fieldKey'], values: [['f1'], ['f2']]},
          ],
        },
        {
          series: [
            {name: 'm2', columns: ['fieldKey'], values: [['f3'], ['f4']]},
          ],
        },
      ],
    }
    const result = parseShowFieldKeys(response)
    expect(result.errors).toEqual([])
    expect(result.fieldSets).toEqual({
      m1: ['f1', 'f2'],
      m2: ['f3', 'f4'],
    })
  })

  it('parses multiple errors', () => {
    const response = {
      results: [
        {error: 'measurement not found: m1'},
        {error: 'measurement not found: m2'},
      ],
    }
    const result = parseShowFieldKeys(response)
    expect(result.errors).toEqual([
      'measurement not found: m1',
      'measurement not found: m2',
    ])
    expect(result.fieldSets).toEqual({})
  })

  it('parses a mix of results and errors', () => {
    const response = {
      results: [
        {
          series: [
            {name: 'm1', columns: ['fieldKey'], values: [['f1'], ['f2']]},
          ],
        },
        {error: 'measurement not found: m2'},
      ],
    }
    const result = parseShowFieldKeys(response)
    expect(result.errors).toEqual(['measurement not found: m2'])
    expect(result.fieldSets).toEqual({
      m1: ['f1', 'f2'],
    })
  })
})
