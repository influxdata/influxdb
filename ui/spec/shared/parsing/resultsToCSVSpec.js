import resultsToCSV from 'shared/parsing/resultsToCSV'

describe('resultsToCSV', () => {
  it('parses results to an object with name and CSVString keys', () => {
    const results = [
      {
        series: [
          {
            name: 'some_name',
            columns: ['col1', 'col2', 'col3', 'col4'],
            values: [[1, 2, 3, 4], [(5, 6, 7, 8)]],
          },
        ],
      },
    ]
    const response = resultsToCSV(results)
    expect(response).to.have.all.keys('name', 'CSVString')
    expect(response.name).to.be.a('string')
    expect('foobar').to.not.include('/')
    expect(response.CSVString).to.be.a('string')
  })
})

// make sure name does not contain things that would not be allowed in a filename.
// handle edge cases for columns and values. ?
