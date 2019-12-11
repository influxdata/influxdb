import {getTimeRangeVars} from 'src/variables/utils/getTimeRangeVars'
import {pastHourTimeRange} from 'src/shared/constants/timeRanges'

const custom = 'custom' as 'custom'

describe('getTimeRangeVars', () => {
  test('should handle relative lower dates', () => {
    const actual = getTimeRangeVars(pastHourTimeRange)

    const init = actual[0].init as any

    expect(init.type).toEqual('UnaryExpression')
    expect(init.operator).toEqual('-')
    expect(init.argument.type).toEqual('DurationLiteral')
    expect(init.argument.values).toEqual([{magnitude: 1, unit: 'h'}])
  })

  test('should handle custom lower dates', () => {
    const timeRange = {
      type: custom,
      lower: '2019-02-28T15:00:00Z',
      upper: '2019-03-28T15:00:00Z',
    }

    const actual = getTimeRangeVars(timeRange)

    expect(actual[0].init.type).toEqual('DateTimeLiteral')
    expect((actual[0].init as any).value).toEqual('2019-02-28T15:00:00.000Z')
  })

  test('should handle absolute upper dates', () => {
    const timeRange = {
      type: custom,
      lower: '2019-02-26T15:00:00Z',
      upper: '2019-02-27T15:00:00Z',
    }

    const actual = getTimeRangeVars(timeRange)

    expect(actual[1].init.type).toEqual('DateTimeLiteral')
    expect((actual[1].init as any).value).toEqual('2019-02-27T15:00:00.000Z')
  })

  test('should set non-existent upper dates to now', () => {
    const actual = getTimeRangeVars(pastHourTimeRange)

    expect(actual[1].init).toEqual({
      type: 'CallExpression',
      callee: {
        type: 'Identifier',
        name: 'now',
      },
    })
  })
})
