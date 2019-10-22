const {formatStatic, formatBase} = require('./env')

describe('enviroment normalization', () => {
  describe('static path formatter', () => {
    it('should strip the first slash', () => {
      expect(formatStatic('/neateo/')).toBe('neateo/')
    })

    it('should not strip the last slash', () => {
      expect(formatStatic('neateo/')).toBe('neateo/')
    })

    it('should ignore middle slashes', () => {
      expect(formatStatic('n/ea/teo')).toBe('n/ea/teo/')
    })

    it('should add a final slash here', () => {
      expect(formatStatic('neateo')).toBe('neateo/')
    })
  })

  describe('base path formatter', () => {
    it('should ignore properly formatted things', () => {
      expect(formatBase('/neateo/')).toBe('/neateo/')
    })

    it('should add a slash at the beginning', () => {
      expect(formatBase('neateo/')).toBe('/neateo/')
    })

    it('should add a slash at the end', () => {
      expect(formatBase('/neateo')).toBe('/neateo/')
    })
  })
})
