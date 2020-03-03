import {
  generateImport,
  formatFunctionForInsert,
} from 'src/timeMachine/utils/insertFunction'

describe('insertFunction', () => {
  test('generateImport', () => {
    const emptyImport = generateImport('', '')
    expect(emptyImport).toEqual(false)
    const func = 'aggregateWindow'
    const script = `from(bucket: "b0")
    |> range(start: v.timeRangeStart, stop: v.timeRangeStop)
    |> filter(fn: (r) => r._measurement == "m0")`
    const actual = generateImport(func, script)
    expect(actual).toEqual(`import "${func}"`)
  })

  test('formatFunctionForInsert', () => {
    const fluxFunc = 'funky'
    const union = 'union'
    const requiresNewLine = formatFunctionForInsert(union, fluxFunc)
    expect(requiresNewLine).toEqual(`\n${fluxFunc}`)
    const to = 'to'
    const fluxNewLine = formatFunctionForInsert(to, fluxFunc)
    expect(fluxNewLine).toEqual(`\n  |> ${fluxFunc}`)
  })
})
