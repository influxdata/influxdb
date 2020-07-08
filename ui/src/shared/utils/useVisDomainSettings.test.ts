// Funcs
import {
  getValidRange,
  getRemainingRange,
} from 'src/shared/utils/useVisDomainSettings'

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

  it('should return the start and end times based on the data array if no start / endTime are passed', () => {
    expect(getValidRange(data, null)).toEqual([data[0], data[data.length - 1]])
  })
})

describe('getRemainingRange', () => {
  const startTime: string = '2019-11-07T02:46:51Z'
  const unixStart: number = 1573094811000
  const endTime: string = '2019-11-28T14:46:51Z'

  it('should return null when no parameters are input', () => {
    expect(getRemainingRange(undefined, undefined, undefined)).toEqual(null)
  })

  it('should return null when no data is passed', () => {
    const timeRange: CustomTimeRange = {
      type: 'custom',
      lower: startTime,
      upper: endTime,
    }
    expect(getRemainingRange([], timeRange, [null, null])).toEqual(null)
  })

  describe('should return a valid number from the stored domain as the min y-axis', () => {
    const timeRange: CustomTimeRange = {
      type: 'custom',
      lower: startTime,
      upper: endTime,
    }

    it('should handle a positive integer', () => {
      const setMin = unixStart - 10
      const [start] = getRemainingRange(data, timeRange, [setMin, null])
      expect(start).toEqual(setMin)
    })

    it('should handle a negative integer', () => {
      const setMin = -10
      const [start] = getRemainingRange(data, timeRange, [setMin, null])
      expect(start).toEqual(setMin)
    })

    it('should handle a positive float', () => {
      const setMin = Math.PI
      const [start] = getRemainingRange(data, timeRange, [setMin, null])
      expect(start).toEqual(setMin)
    })

    it('should handle a negative float', () => {
      const setMin = -35.67
      const [start] = getRemainingRange(data, timeRange, [setMin, null])
      expect(start).toEqual(setMin)
    })

    it('should handle zero', () => {
      const setMin = 0
      const [start] = getRemainingRange(data, timeRange, [setMin, null])
      expect(start).toEqual(setMin)
    })

    it('should handle positive zero', () => {
      const setMin = +0
      const [start] = getRemainingRange(data, timeRange, [setMin, null])
      expect(start).toEqual(setMin)
    })

    it('should handle negative zero', () => {
      const setMin = -0
      const [start] = getRemainingRange(data, timeRange, [setMin, null])
      expect(start).toEqual(setMin)
    })
  })

  describe('should return the min value of the data set if stored domain is not set to valid number', () => {
    let dummyData = [6, 1, 10, 20]
    let timeRange = null

    it('should handle NaN', () => {
      const setMin = NaN
      const [start] = getRemainingRange(dummyData, timeRange, [setMin, null])
      expect(start).toEqual(Math.min(...dummyData))
    })

    it('should handle Infinity', () => {
      const setMin = Infinity
      const [start] = getRemainingRange(dummyData, timeRange, [setMin, null])
      expect(start).toEqual(Math.min(...dummyData))
    })

    it('should handle positive Infinity', () => {
      const setMin = +Infinity
      const [start] = getRemainingRange(dummyData, timeRange, [setMin, null])
      expect(start).toEqual(Math.min(...dummyData))
    })

    it('should handle negative Infinity', () => {
      const setMin = -Infinity
      const [start] = getRemainingRange(dummyData, timeRange, [setMin, null])
      expect(start).toEqual(Math.min(...dummyData))
    })

    it('should include time range for determining min value', () => {
      timeRange = {
        type: 'custom',
        lower: startTime,
        upper: endTime,
      } as CustomTimeRange
      const startTimeValue = Date.parse(startTime)
      const endTimeValue = Date.parse(endTime)
      dummyData = [startTimeValue + 10, endTimeValue - 10]
      const setMin = NaN
      const [start] = getRemainingRange(dummyData, timeRange, [setMin, null])
      expect(start).toEqual(startTimeValue)
    })
  })

  describe('should return a valid number from the stored domain as the max y-axis', () => {
    const timeRange = null
    it('should handle a positive integer', () => {
      const setMax = unixStart - 10
      const range = getRemainingRange(data, timeRange, [null, setMax])
      expect(range[1]).toEqual(setMax)
    })

    it('should handle a negative integer', () => {
      const setMax = -10
      const range = getRemainingRange(data, timeRange, [null, setMax])
      expect(range[1]).toEqual(setMax)
    })

    it('should handle a positive float', () => {
      const setMax = Math.PI
      const range = getRemainingRange(data, timeRange, [null, setMax])
      expect(range[1]).toEqual(setMax)
    })

    it('should handle a negative float', () => {
      const setMax = -35.67
      const range = getRemainingRange(data, timeRange, [null, setMax])
      expect(range[1]).toEqual(setMax)
    })

    it('should handle zero', () => {
      const setMax = 0
      const range = getRemainingRange(data, timeRange, [null, setMax])
      expect(range[1]).toEqual(setMax)
    })

    it('should handle positive zero', () => {
      const setMax = +0
      const range = getRemainingRange(data, timeRange, [null, setMax])
      expect(range[1]).toEqual(setMax)
    })

    it('should handle negative zero', () => {
      const setMax = -0
      const range = getRemainingRange(data, timeRange, [null, setMax])
      expect(range[1]).toEqual(setMax)
    })
  })

  describe('should return the max value of the data set if stored domain is not set to valid number', () => {
    let dummyData = [6, 100, 10, 20]
    let timeRange = null

    it('should handle NaN', () => {
      const setMax = NaN
      const range = getRemainingRange(dummyData, timeRange, [null, setMax])
      expect(range[1]).toEqual(Math.max(...dummyData))
    })

    it('should handle Infinity', () => {
      const setMax = Infinity
      const range = getRemainingRange(dummyData, timeRange, [null, setMax])
      expect(range[1]).toEqual(Math.max(...dummyData))
    })

    it('should handle positive Infinity', () => {
      const setMax = +Infinity
      const range = getRemainingRange(dummyData, timeRange, [null, setMax])
      expect(range[1]).toEqual(Math.max(...dummyData))
    })

    it('should handle negative Infinity', () => {
      const setMax = -Infinity
      const range = getRemainingRange(dummyData, timeRange, [null, setMax])
      expect(range[1]).toEqual(Math.max(...dummyData))
    })

    it('should include time range for determining max value', () => {
      timeRange = {
        type: 'custom',
        lower: startTime,
        upper: endTime,
      } as CustomTimeRange
      const startTimeValue = Date.parse(startTime)
      const endTimeValue = Date.parse(endTime)
      dummyData = [startTimeValue + 10, endTimeValue - 10]
      const setMax = NaN
      const range = getRemainingRange(dummyData, timeRange, [null, setMax])
      expect(range[1]).toEqual(endTimeValue)
    })
  })
})
