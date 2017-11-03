import {timeRangeType, shiftTimeRange} from 'shared/query/helpers'
import {ABSOLUTE} from 'shared/constants/timeRange'
const format = 'influxql'

describe.only('Shared.Query.Helpers', () => {
  describe('timeRangeTypes', () => {
    it('return invlalid if no upper and lower', () => {
      const upper = null
      const lower = null

      const timeRange = {lower, upper}

      expect(timeRangeType(timeRange)).to.equal(ABSOLUTE)
    })

    it('it can detect absolute type', () => {
      const tenMinutes = 600000
      const upper = Date.now()
      const lower = upper - tenMinutes

      const timeRange = {lower, upper, format}

      expect(timeRangeType(timeRange)).to.equal(ABSOLUTE)
    })
  })
})
