// Funcs
import {getValidRange} from 'src/shared/utils/useVisDomainSettings'

// Types
import {numericColumnData as data} from 'mocks/dummyData'

describe('getValidRange', () => {
  const startTime: number = 1573123611000
  const endTime: number = 1574981211000
  it('should return null when no parameters are input', () => {
    expect(getValidRange()).toEqual(null)
  })
  it('should return null when no data is passed', () => {
    expect(getValidRange([], startTime, endTime)).toEqual(null)
  })
  it("should return the startTime as startTime if it's before the first time in the data array", () => {
    const [start] = getValidRange(data, startTime, endTime)
    expect(start).toEqual(startTime)
    const [beginning] = getValidRange(data, endTime, endTime)
    expect(beginning).toEqual(data[0])
  })
  it("should return the endTime as endTime if it's before the last time in the data array", () => {
    const range = getValidRange(data, startTime, endTime)
    expect(range[1]).toEqual(endTime)
    const newRange = getValidRange(data, endTime, startTime)
    expect(newRange[1]).toEqual(data[data.length - 1])
  })
  it('should return the the start and end times based on the data array if no start / endTime are passed', () => {
    expect(getValidRange(data)).toEqual([data[0], data[data.length - 1]])
  })
})
