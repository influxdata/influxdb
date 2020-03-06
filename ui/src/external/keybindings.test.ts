import {isCommented, toggleCommenting} from 'src/external/monaco.flux.hotkeys'

const TEST_CASES = [
  ['//moo', true],
  ['     //foo', true],
  ['     // foo', true],
  ['ss// foo', false],
  ['           //    foo', true],
  ['lolfoo', false],
  ['lolfoo //', false],
  ['///', true],
]

describe('isCommented function', () => {
  test.each(TEST_CASES)(
    'returns true / false if text is commented or not',
    (input, expected) => {
      expect(isCommented(input)).toEqual(expected)
    }
  )
})

describe('toggleCommenting function', () => {
  test('can add comment', () => {
    const s = 'moo'
    expect(toggleCommenting(s, true)).toEqual(`// ${s}`)
  })
  test('can remove comment', () => {
    const s = 'moo'
    expect(toggleCommenting(`// ${s}`, false)).toEqual(s)
  })
})
