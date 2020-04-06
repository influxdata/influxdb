import {generateImport} from 'src/timeMachine/utils/insertFunction'

describe('insertFunction', () => {
  test('generateImport should generate an import statement', () => {
    const emptyImport = generateImport('', '')
    expect(emptyImport).toEqual(false)
    const func = 'aggregateWindow'
    const script = `from(bucket: "b0")
    |> range(start: v.timeRangeStart, stop: v.timeRangeStop)
    |> filter(fn: (r) => r._measurement == "m0")`
    const actual = generateImport(func, script)
    expect(actual).toEqual(`import "${func}"`)
  })
})
