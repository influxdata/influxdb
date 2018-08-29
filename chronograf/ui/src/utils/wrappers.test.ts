import {getDeep} from 'src/utils/wrappers'

describe('utils.wrappers', () => {
  describe('get', () => {
    it('gets a nested value', () => {
      const example = {a: {b: 'hello'}}

      expect(getDeep(example, 'a.b', 'default')).toEqual('hello')
    })

    it('gets the default value when key is empty', () => {
      const example = {a: {b: 'hello'}}

      expect(getDeep(example, 'a.c', 'default')).toEqual('default')
    })
  })
})
