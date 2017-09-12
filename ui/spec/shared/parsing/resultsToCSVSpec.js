import resultsToCSV, {formatDate} from 'shared/parsing/resultsToCSV'

describe('resultsToCSV', () => {
  it('parses results to an object with name and CSVString keys', () => {
    const results = [
      {
        series: [
          {
            name: 'some_name',
            columns: ['col1', 'col2', 'col3', 'col4'],
            values: [[1000000000, '2', 3, 4], [2000000000, '6', 7, 8]],
          },
        ],
      },
    ]
    const response = resultsToCSV(results)
    const expected = `date,col2,col3,col4\n${formatDate(
      1000000000
    )},2,3,4\n${formatDate(2000000000)},6,7,8`
    expect(response).to.have.all.keys('name', 'CSVString')
    expect(response.name).to.be.a('string')
    expect(response.CSVString).to.be.a('string')
    expect(response.CSVString).to.equal(expected)
  })
})

describe('formatDate', () => {
  it('converts timestamp to an excel compatible date string', () => {
    const timestamp = 1000000000000
    const result = formatDate(timestamp)
    expect(result).to.be.a('string')
    expect(+new Date(result)).to.equal(timestamp)
  })
})
