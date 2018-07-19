import moment from 'moment'
import * as time from 'src/dashboards/utils/time'

describe('dashboards.utils.time', () => {
  describe('millisecondTimeRange', () => {
    it('when upper is now() returns valid dates', () => {
      const expectedNow = moment()
        .subtract()
        .seconds(1)
        .unix()
      const timeRange = {upper: 'now()', lower: moment().format()}
      const result = time.millisecondTimeRange(timeRange)

      expect(result.since).toBeGreaterThanOrEqual(expectedNow)
      expect(result.until).toBeGreaterThanOrEqual(expectedNow)
    })

    it('when seconds is present returns valid dates', () => {
      const timeRange = {seconds: 10}
      const expectedSince = moment()
        .subtract()
        .seconds(10)
        .unix()
      const result = time.millisecondTimeRange(timeRange)

      expect(result.since).toBeGreaterThanOrEqual(expectedSince)
      expect(result.until).toBe(null)
    })
  })
})
