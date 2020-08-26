import {exportVariables} from 'src/variables/utils/exportVariables'
// Mocks
import {createVariable} from 'src/variables/mocks'

describe('exportVariables', () => {
  it('should find dependent variables', () => {
    const a = createVariable('a', 'f(x: v.b)')
    const b = createVariable('b', 'cool')
    const c = createVariable('c', 'nooo!')

    const vars = [a, b, c]

    const actual = exportVariables([a], vars)

    expect(actual).toEqual([a, b])
  })

  it('should find dependent variables with cycles', () => {
    const a = createVariable('a', 'f(x: v.b, y: v.c)')
    const b = createVariable('b', 'f(x: v.f, y: v.e)')
    const c = createVariable('c', 'f(x: v.g)')
    const d = createVariable('d', 'nooooo!')
    const e = createVariable('e', 'pick')
    const f = createVariable('f', 'f(x: v.a, y: v.b)')
    const g = createVariable('g', 'yay')
    const h = createVariable('h', 'nooooo!')

    const vars = [a, b, c, d, e, f, g, h]

    const actual = exportVariables([a], vars)
    const expected = new Set([a, b, c, e, f, g])

    expect(new Set(actual)).toEqual(expected)
  })

  const examples = [
    createVariable('alone', 'v.target'),
    createVariable('space', '\tv.target\n'),
    createVariable('func', 'f(x: v.target)'),
    createVariable('brackets', '[v.target, other]'),
    createVariable('braces', '(v.target)'),
    createVariable('add', '1+v.target-2'),
    createVariable('mult', '1*v.target/2'),
    createVariable('mod', '1+v.target%2'),
    createVariable('bool', '1>v.target<2'),
    createVariable('assignment', 'x=v.target\n'),
    createVariable('curly', '{beep:v.target}\n'),
    createVariable('arrow', '(r)=>v.target==r.field\n'),
    createVariable('comment', '\nv.target//wat?'),
    createVariable('not equal', 'v.target!=r.field'),
    createVariable('like', 'other=~v.target'),
  ]

  examples.forEach(example => {
    it(`should filter vars with shared prefix: ${example.name}`, () => {
      const target = createVariable('target', 'match me!')
      const partial = createVariable('tar', 'broke!')
      const vars = [example, target, partial]

      const actual = exportVariables([example], vars)

      expect(actual).toEqual([example, target])
    })
  })
})
