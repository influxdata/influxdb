import {timeRangeType, shiftTimeRange} from 'shared/query/helpers'
import moment from 'moment'
import {
  INVALID,
  ABSOLUTE,
  INFLUXQL,
  RELATIVE_LOWER,
  RELATIVE_UPPER,
} from 'shared/constants/timeRange'
const format = INFLUXQL

describe('Shared.Query.Helpers', () => {
  describe('timeRangeTypes', () => {
    it('returns invalid if no upper and lower', () => {
      const upper = null
      const lower = null

      const timeRange = {lower, upper}

      expect(timeRangeType(timeRange)).to.equal(INVALID)
    })

    it('can detect absolute type', () => {
      const tenMinutes = 600000
      const upper = Date.now()
      const lower = upper - tenMinutes

      const timeRange = {lower, upper, format}

      expect(timeRangeType(timeRange)).to.equal(ABSOLUTE)
    })

    it('can detect exclusive relative lower', () => {
      const lower = 'now() - 15m'
      const upper = null

      const timeRange = {lower, upper, format}

      expect(timeRangeType(timeRange)).to.equal(RELATIVE_LOWER)
    })

    it('can detect relative upper', () => {
      const upper = 'now()'
      const oneMinute = 60000
      const lower = Date.now() - oneMinute

      const timeRange = {lower, upper, format}

      expect(timeRangeType(timeRange)).to.equal(RELATIVE_UPPER)
    })
  })

  describe('timeRangeShift', () => {
    it('can calculate the shift for absolute timeRanges', () => {
      const upper = Date.now()
      const oneMinute = 60000
      const lower = Date.now() - oneMinute
      const shift = {multiple: 7, unit: 'd'}
      const timeRange = {upper, lower}

      const type = timeRangeType(timeRange)
      const actual = shiftTimeRange(timeRange, shift)
      const expected = {
        lower: `${lower} - 7d`,
        upper: `${upper} - 7d`,
        type: 'shifted',
      }

      expect(type).to.equal(ABSOLUTE)
      expect(actual).to.deep.equal(expected)
    })

    it('can calculate the shift for relative lower timeRanges', () => {
      const shift = {multiple: 7, unit: 'd'}
      const lower = 'now() - 15m'
      const timeRange = {lower, upper: null}

      const type = timeRangeType(timeRange)
      const actual = shiftTimeRange(timeRange, shift)
      const expected = {
        lower: `${lower} - 7d`,
        upper: `now() - 7d`,
        type: 'shifted',
      }

      expect(type).to.equal(RELATIVE_LOWER)
      expect(actual).to.deep.equal(expected)
    })

    it('can calculate the shift for relative upper timeRanges', () => {
      const upper = Date.now()
      const oneMinute = 60000
      const lower = Date.now() - oneMinute
      const shift = {multiple: 7, unit: 'd'}
      const timeRange = {upper, lower}

      const type = timeRangeType(timeRange)
      const actual = shiftTimeRange(timeRange, shift)
      const expected = {
        lower: `${lower} - 7d`,
        upper: `${upper} - 7d`,
        type: 'shifted',
      }

      expect(type).to.equal(ABSOLUTE)
      expect(actual).to.deep.equal(expected)
    })
  })
})
