import showDatabases from 'shared/parsing/showDatabases'

describe('showDatabases', () => {
  it('exposes all the database properties', () => {
    const response = {
      results: [
        {series: [{columns: ['name'], values: [['mydb1'], ['mydb2']]}]},
      ],
    }

    const result = showDatabases(response)

    expect(result.errors).toEqual([])
    expect(result.databases.length).toBe(2)
    expect(result.databases[0]).toBe('mydb1')
    expect(result.databases[1]).toBe('mydb2')
  })

  it('returns an empty array when there are no databases', () => {
    const response = {results: [{series: [{columns: ['name']}]}]}

    const result = showDatabases(response)

    expect(result.errors).toEqual([])
    expect(result.databases).toEqual([])
  })

  it('exposes the server error', () => {
    const response = {results: [{error: 'internal server error?'}]}

    const result = showDatabases(response)

    expect(result.errors).toEqual(['internal server error?'])
    expect(result.databases).toEqual([])
  })
})
