import {
  findHoverTimeIndex,
  getUnixISODiff,
} from 'src/dashboards/utils/tableGraph'

describe('Dashboards.utils.tableGraph', () => {
  describe('findHoverTimeIndex', () => {
    const sortedTimeValues = [
      '_time',
      '2019-01-03T16:02:22Z',
      '2019-01-03T16:03:33Z',
      '2019-01-03T16:05:55Z',
      '2019-01-03T16:07:77Z',
    ]

    it('can fin the min diff', () => {
      const hoverTime = new Date('2019-01-03T16:05:00Z').valueOf()
      const actual = findHoverTimeIndex(sortedTimeValues, hoverTime)

      expect(actual).toEqual(3)
    })

    it('can handle no sortedValues', () => {
      const hoverTime = new Date('2019-01-03T16:05:00Z').valueOf()
      const actual = findHoverTimeIndex(['_time'], hoverTime)

      expect(actual).toEqual(-1)
    })
  })

  describe('getUnixISODiff', () => {
    it('can diff the miliseconds between an epoch ms number and iso string', () => {
      const unixMs = new Date('2019-01-03T16:05:55Z').valueOf()
      const isoUTCTime = '2019-01-03T16:05:00Z'

      const actual = getUnixISODiff(unixMs, isoUTCTime)
      expect(actual).toEqual(55000)
    })
  })
})
