import {move} from 'src/shared/utils/move'

describe('Array.utils.move', () => {
  it('can move an index to itself', () => {
    const actual = move(['a', 'b', 'c'], 2, 2)
    expect(actual).toEqual(['a', 'b', 'c'])
  })

  it('can move an index down', () => {
    const actual = move(['a', 'b', 'c', 'd'], 2, 0)
    expect(actual).toEqual(['c', 'a', 'b', 'd'])
  })

  it('can move an index up', () => {
    const actual = move(['a', 'b', 'c', 'd'], 1, 2)
    expect(actual).toEqual(['a', 'c', 'b', 'd'])
  })
})
