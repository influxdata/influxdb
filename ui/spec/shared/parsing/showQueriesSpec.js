import showQueriesParser from 'shared/parsing/showQueries'

describe('showQueriesParser', () => {
  it('exposes all currently running queries', () => {
    const response = {
      results: [
        {
          series: [
            {
              columns: ['qid', 'query', 'database', 'duration'],
              values: [
                [1, 'SHOW QUERIES', 'db1', '1s'],
                [2, 'SELECT foo FROM bar', 'db1', '2s'],
              ],
            },
          ],
        },
      ],
    }

    const result = showQueriesParser(response)

    expect(result.errors).to.eql([])
    expect(result.queries.length).to.equal(2)
    expect(result.queries[0]).to.eql({
      id: 1,
      database: 'db1',
      query: 'SHOW QUERIES',
      duration: '1s',
    })
    expect(result.queries[1]).to.eql({
      id: 2,
      database: 'db1',
      query: 'SELECT foo FROM bar',
      duration: '2s',
    })
    expect({foo: 'bar'}).to.eql({foo: 'bar'})
  })

  it('exposes the server error', () => {
    const response = {results: [{error: 'internal server error?'}]}

    const result = showQueriesParser(response)

    expect(result.errors).to.eql(['internal server error?'])
    expect(result.queries).to.eql([])
  })
})
