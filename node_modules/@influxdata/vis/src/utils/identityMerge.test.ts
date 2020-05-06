import {
  identityMerge,
  enumeratePaths,
  getByPath,
  setByPath,
  isEqual,
} from './identityMerge'

describe('identityMerge', () => {
  test('can merge two objects', () => {
    const source = {
      key1: 'PRIMATIVE_VALUE',
      key2: 10,
      key3: ['a', 'b', 'c'],
      key4: {
        key5: 'd',
        key6: 20,
        key7: {
          key8: 'e',
          key9: ['f', 'g', {key10: 'h'}],
        },
      },
    }

    const target = {
      ...source,
      key2: 'CHANGED_PRIMATIVE_VALUE',
      key3: ['a', 'b', 'c'],
      key4: {
        ...source.key4,
        key7: {
          key8: 'i',
          key9: ['f', 'g', {key10: 'h'}],
        },
      },
    }

    const result = identityMerge(source, target)

    expect(result.key1).toBe(source.key1)
    expect(result.key2).toBe('CHANGED_PRIMATIVE_VALUE')
    expect(result.key3).toBe(source.key3)
    expect(result.key4).not.toBe(source.key4)
    expect(result.key4.key5).toBe(source.key4.key5)
    expect(result.key4.key6).toBe(source.key4.key6)
    expect(result.key4.key7).not.toBe(source.key4.key7)
    expect(result.key4.key7.key8).toBe('i')
    expect(result.key4.key7.key9).toBe(source.key4.key7.key9)
  })

  test('returns source when source and target are logically equal', () => {
    const source = {foo: 'a', bar: 'b', baz: ['a', 'b', 'c']}
    const target = {foo: 'a', bar: 'b', baz: ['a', 'b', 'c']}

    const result = identityMerge(source, target)

    expect(result).toBe(source)
  })
})

describe('enumeratePaths', () => {
  test('enumerates the paths in a tree according to preorder traversal', () => {
    const target = {
      root: {
        a: [4, 5, 6],
        b: {
          d: 'E',
        },
      },
    }

    const result = enumeratePaths(target)

    const expected = [
      ['root'],
      ['root', 'a'],
      ['root', 'a', '0'],
      ['root', 'a', '1'],
      ['root', 'a', '2'],
      ['root', 'b'],
      ['root', 'b', 'd'],
    ]

    expect(result).toEqual(expected)
  })
})

describe('getByPath', () => {
  test('can access existant and nonexistant properties', () => {
    expect(getByPath({a: {b: {c: 2}}}, ['a', 'b', 'c'])).toEqual(2)
    expect(getByPath({a: false}, ['a'])).toEqual(false)
    expect(getByPath({a: false}, ['b'])).toBeUndefined()
  })

  test('can safely attempt property access on null, undefined values, and primative values', () => {
    expect(getByPath(null, ['a', 'b'])).toBeUndefined()
    expect(getByPath(undefined, ['a', 'b'])).toBeUndefined()
    expect(getByPath(1, ['a', 'b'])).toBeUndefined()
    expect(getByPath(false, ['a', 'b'])).toBeUndefined()
    expect(getByPath('howdy', ['a', 'b'])).toBeUndefined()
  })
})

describe('setByPath', () => {
  test('can set deep non-existant paths in an object', () => {
    const target = {a: '1'}

    setByPath(target, ['b', 'c'], 2)

    expect(target).toEqual({a: '1', b: {c: 2}})
  })

  test('can set deep existant paths in an object', () => {
    const target = {a: '1', b: {c: 2}}

    setByPath(target, ['b', 'c'], 3)

    expect(target).toEqual({a: '1', b: {c: 3}})
  })

  test('throws an error when attempting to set on null', () => {
    expect(() => setByPath(null, ['a', 'b'], 2)).toThrow()
  })

  test('throws an error when attempting to set an empty path', () => {
    expect(() => setByPath({a: 'b'}, [], 2)).toThrow()
  })
})

describe('isEqual', () => {
  test('checks if objects are equal according to their logical identity', () => {
    expect(isEqual('4', '4')).toBeTruthy()
    expect(isEqual('4', '5')).toBeFalsy()
    expect(isEqual('4', 4)).toBeFalsy()
    expect(isEqual('4', 4)).toBeFalsy()

    expect(
      isEqual({a: '2', b: [1, 2, {c: 3}]}, {a: '2', b: [1, 2, {c: 3}]})
    ).toBeTruthy()

    expect(
      isEqual({a: '2', b: [1, 2, {c: 3}]}, {a: '2', b: [1, 2, {c: 4}]})
    ).toBeFalsy()

    expect(isEqual(() => 2, () => 2)).toBeFalsy()

    const f = () => 2

    expect(isEqual(f, f)).toBeTruthy()
  })
})
