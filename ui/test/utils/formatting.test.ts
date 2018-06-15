import {formatRPDuration} from 'src/utils/formatting'

describe('Formatting helpers', () => {
  describe('formatRPDuration', () => {
    it("returns 'infinite' for a retention policy with a value of '0'", () => {
      const actual = formatRPDuration('0')

      expect(actual).toBe('∞')
    })

    it('correctly formats retention policy durations', () => {
      expect(formatRPDuration('24h0m0s')).toBe('24h')

      expect(formatRPDuration('168h0m0s')).toBe('7d')

      expect(formatRPDuration('200h32m3s')).toBe('8d8h32m3s')
    })
  })
})
