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
})
