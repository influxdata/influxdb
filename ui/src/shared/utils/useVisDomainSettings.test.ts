// Funcs
import {getValidRange} from 'src/shared/utils/useVisDomainSettings'

// Types
import {numericColumnData as data} from 'mocks/dummyData'
import {CustomTimeRange} from 'src/types/queries'

describe('getValidRange', () => {
  // const startTime: string = 'Nov 07 2019 02:46:51 GMT-0800'
  const startTime: string = '2019-11-07T02:46:51Z'
  const unixStart: number = 1573094811000
  const endTime: string = '2019-11-28T14:46:51Z'
  const unixEnd: number = 1574952411000
  it('should return null when no parameters are input', () => {
    expect(getValidRange(undefined, undefined)).toEqual(null)
  })
  it('should return null when no data is passed', () => {
    const timeRange: CustomTimeRange = {
      type: 'custom',
      lower: startTime,
      upper: endTime,
    }
    expect(getValidRange([], timeRange)).toEqual(null)
  })
  it("should return the startTime as startTime if it's before the first time in the data array", () => {
    const timeRange: CustomTimeRange = {
      type: 'custom',
      lower: startTime,
      upper: endTime,
    }
    const [start] = getValidRange(data, timeRange)
    expect(start).toEqual(unixStart)
    timeRange.lower = endTime
    const [beginning] = getValidRange(data, timeRange)
    expect(beginning).toEqual(data[0])
  })
  it("should return the endTime as endTime if it's before the last time in the data array", () => {
    const timeRange: CustomTimeRange = {
      type: 'custom',
      lower: startTime,
      upper: endTime,
    }
    const range = getValidRange(data, timeRange)
    expect(range[1]).toEqual(unixEnd)
    timeRange.lower = endTime
    timeRange.upper = startTime
    const newRange = getValidRange(data, timeRange)
    expect(newRange[1]).toEqual(data[data.length - 1])
  })
  it('should return the the start and end times based on the data array if no start / endTime are passed', () => {
    expect(getValidRange(data, null)).toEqual([data[0], data[data.length - 1]])
  })
})
