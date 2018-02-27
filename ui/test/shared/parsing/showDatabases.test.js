import showDatabases from 'shared/parsing/showDatabases'

describe('showDatabases', () => {
  it('exposes all the database properties', () => {
    const response = {
      results: [
        {series: [{columns: ['name'], values: [['mydb1'], ['mydb2']]}]},
      ],
    }

    const result = showDatabases(response)

    expect(result.errors).to.deep.equal([])
    expect(result.databases.length).to.equal(2)
    expect(result.databases[0]).to.equal('mydb1')
    expect(result.databases[1]).to.equal('mydb2')
  })

  it('returns an empty array when there are no databases', () => {
    const response = {results: [{series: [{columns: ['name']}]}]}

    const result = showDatabases(response)

    expect(result.errors).to.deep.equal([])
    expect(result.databases).to.deep.equal([])
  })

  it('exposes the server error', () => {
    const response = {results: [{error: 'internal server error?'}]}

    const result = showDatabases(response)

    expect(result.errors).to.deep.equal(['internal server error?'])
    expect(result.databases).to.deep.equal([])
  })
})
