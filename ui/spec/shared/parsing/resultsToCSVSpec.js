import {resultsToCSV, formatDate} from 'shared/parsing/resultsToCSV'

describe('formatDate', () => {
  it('converts timestamp to an excel compatible date string', () => {
    const timestamp = 1000000000000
    const result = formatDate(timestamp)
    expect(result).to.be.a('string')
    expect(+new Date(result)).to.equal(timestamp)
  })
})

describe('resultsToCSV', () => {
  it('parses results, a time series data structure, to an object with name and CSVString keys', () => {
    const results = [
      {
        statement_id: 0,
        series: [
          {
            name: 'procstat',
            columns: ['time', 'mean_cpu_usage'],
            values: [
              [1505262600000, 0.06163066773148772],
              [1505264400000, 2.616484718180463],
              [1505266200000, 1.6174323943535571],
            ],
          },
        ],
      },
    ]
    const response = resultsToCSV(results)
    const expected = {
      name: 'procstat',
      CSVString: `date,mean_cpu_usage\n${formatDate(
        1505262600000
      )},0.06163066773148772\n${formatDate(
        1505264400000
      )},2.616484718180463\n${formatDate(1505266200000)},1.6174323943535571`,
    }
    expect(response).to.have.all.keys('flag', 'name', 'CSVString')
    expect(response.flag).to.be.a('string')
    expect(response.name).to.equal('ok')
    expect(response.name).to.be.a('string')
    expect(response.CSVString).to.be.a('string')
    expect(response.name).to.equal(expected.name)
    expect(response.CSVString).to.equal(expected.CSVString)
  })
})
