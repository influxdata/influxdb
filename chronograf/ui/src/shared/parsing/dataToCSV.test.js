import {dataToCSV, formatDate} from 'shared/parsing/dataToCSV'
import moment from 'moment'

describe('formatDate', () => {
  it('converts timestamp to an excel compatible date string', () => {
    const timestamp = 1000000000000
    const result = formatDate(timestamp)
    expect(moment(result, 'M/D/YYYY h:mm:ss.SSSSSSSSS A').valueOf()).toBe(
      timestamp
    )
  })
})

describe('dataToCSV', () => {
  it('parses data, an array of arrays, to a csv string', () => {
    const data = [[1, 2], [3, 4], [5, 6], [7, 8]]
    const returned = dataToCSV(data)
    const expected = `1,2\n3,4\n5,6\n7,8`

    expect(returned).toEqual(expected)
  })

  it('converts values to dates if title of first column is time.', () => {
    const data = [
      ['time', 'something'],
      [1505262600000, 0.06163066773148772],
      [1505264400000, 2.616484718180463],
      [1505266200000, 1.6174323943535571],
    ]
    const returned = dataToCSV(data)
    const expected = `date,something\n${formatDate(
      1505262600000
    )},0.06163066773148772\n${formatDate(
      1505264400000
    )},2.616484718180463\n${formatDate(1505266200000)},1.6174323943535571`

    expect(returned).toEqual(expected)
  })

  it('returns an empty string if data is empty', () => {
    const data = [[]]
    const returned = dataToCSV(data)
    const expected = ''
    expect(returned).toEqual(expected)
  })
})
