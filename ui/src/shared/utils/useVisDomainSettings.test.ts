// Funcs
import {getValidRange} from 'src/shared/utils/useVisDomainSettings'

// Types
import {NumericColumnData} from '@influxdata/giraffe'

describe('getValidRange', () => {
  const data: NumericColumnData = [
    1573766950000,
    1573766950000,
    1573766960000,
    1573766970000,
    1573766980000,
    1573766990000,
    1573767000000,
    1573767010000,
    1573767020000,
    1573767030000,
    1573767040000,
    1573767050000,
    1573767060000,
    1573767070000,
    1573767080000,
    1573767090000,
    1573767100000,
    1573767110000,
    1573767120000,
    1573767130000,
    1573767140000,
    1573767150000,
    1573767160000,
    1573767170000,
    1573767180000,
    1573767190000,
    1573767200000,
    1573767210000,
    1573767220000,
    1573767230000,
    1573767240000,
    1573767250000,
    1573767260000,
    1573767270000,
    1573767280000,
    1573767290000,
    1573767300000,
    1573767310000,
    1573767320000,
    1573767330000,
    1573767340000,
    1573767350000,
    1573767360000,
    1573767370000,
    1573767380000,
    1573767390000,
    1573767400000,
    1573767410000,
    1573767420000,
    1573767430000,
    1573767440000,
    1573767450000,
    1573767460000,
    1573767470000,
    1573767480000,
    1573767490000,
    1573767500000,
    1573767510000,
    1573767520000,
    1573767530000,
    1573767540000,
    1573767550000,
    1573767560000,
    1573767570000,
    1573767580000,
    1573767590000,
    1573767600000,
    1573767610000,
    1573767620000,
    1573767630000,
    1573767640000,
    1573767650000,
    1573767660000,
    1573767670000,
    1573767680000,
    1573767690000,
    1573767700000,
    1573767710000,
    1573767720000,
    1573767730000,
    1573767740000,
    1573767750000,
    1573767760000,
    1573767770000,
    1573767780000,
    1573767790000,
    1573767800000,
    1573767810000,
    1573767820000,
    1573767830000,
    1573767840000,
    1573767850000,
    1573767860000,
    1573767870000,
    1573767880000,
    1573767890000,
    1573767900000,
    1573767910000,
    1573767920000,
    1573767930000,
    1573767940000,
  ]
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
