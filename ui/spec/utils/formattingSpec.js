import {formatBytes, formatRPDuration} from 'utils/formatting'

describe('Formatting helpers', () => {
  describe('formatBytes', () => {
    it('returns null when passed a falsey value', () => {
      const actual = formatBytes(null)

      expect(actual).to.equal(null)
    })

    it('returns the correct value when passed 0', () => {
      const actual = formatBytes(0)

      expect(actual).to.equal('0 Bytes')
    })

    it("converts a raw byte value into it's most appropriate unit", () => {
      expect(formatBytes(1000)).to.equal('1 KB')
      expect(formatBytes(1000000)).to.equal('1 MB')
      expect(formatBytes(1000000000)).to.equal('1 GB')
    })
  })

  describe('formatRPDuration', () => {
    it("returns 'infinite' for a retention policy with a value of '0'", () => {
      const actual = formatRPDuration('0')

      expect(actual).to.equal('∞')
    })

    it('correctly formats retention policy durations', () => {
      expect(formatRPDuration('24h0m0s')).to.equal('24h')

      expect(formatRPDuration('168h0m0s')).to.equal('7d')

      expect(formatRPDuration('200h32m3s')).to.equal('8d8h32m3s')
    })
  })
})
