import {getNested} from 'src/utils/wrappers'

describe('utils.wrappers', () => {
  describe('get', () => {
    it('gets a nested value', () => {
      const example = {a: {b: 'hello'}}

      expect(getNested(example, 'a.b', 'default')).toEqual('hello')
    })

    it('gets the default value when key is empty', () => {
      const example = {a: {b: 'hello'}}

      expect(getNested(example, 'a.c', 'default')).toEqual('default')
    })
  })
})
