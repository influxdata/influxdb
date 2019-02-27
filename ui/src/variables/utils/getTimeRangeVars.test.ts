import {getTimeRangeVars} from 'src/variables/utils/getTimeRangeVars'
import {TimeRange} from 'src/types/v2'

describe('getTimeRangeVars', () => {
  test('should handle relative lower dates', () => {
    const timeRange: TimeRange = {
      lower: 'now() - 1h',
    }

    const actual = getTimeRangeVars(timeRange)

    const init = actual[0].init as any

    expect(init.type).toEqual('UnaryExpression')
    expect(init.operator).toEqual('-')
    expect(init.argument.type).toEqual('DurationLiteral')
    expect(init.argument.values).toEqual([{magnitude: 1, unit: 'h'}])
  })

  test('should handle absolute lower dates', () => {
    const timeRange: TimeRange = {
      lower: '2019-02-28T15:00:00Z',
    }

    const actual = getTimeRangeVars(timeRange)

    expect(actual[0].init.type).toEqual('DateTimeLiteral')
    expect((actual[0].init as any).value).toEqual('2019-02-28T15:00:00.000Z')
  })

  test('should handle absolute upper dates', () => {
    const timeRange: TimeRange = {
      lower: '2019-02-26T15:00:00Z',
      upper: '2019-02-27T15:00:00Z',
    }

    const actual = getTimeRangeVars(timeRange)

    expect(actual[1].init.type).toEqual('DateTimeLiteral')
    expect((actual[1].init as any).value).toEqual('2019-02-27T15:00:00.000Z')
  })

  test('should handle non-existant upper dates', () => {
    const timeRange: TimeRange = {
      lower: 'now() - 1h',
    }

    const actual = getTimeRangeVars(timeRange)

    expect(actual[1].init).toEqual({
      type: 'CallExpression',
      callee: {
        type: 'Identifier',
        name: 'now',
      },
    })
  })
})
