import {numFunctions} from 'shared/reducers/helpers/field'

describe('Formatting helpers', () => {
  describe('formatBytes', () => {
    it('returns null when passed a falsey value', () => {
      const actual = numFunctions(null)
      expect(actual).to.equal(0)
    })
  })
})
