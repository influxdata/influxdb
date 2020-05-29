import {getWindowPeriodVariable} from 'src/variables/utils/getWindowVars'
import {defaultVariableAssignments} from 'src/variables/mocks'

describe('getWindowPeriodVariable', () => {
  beforeEach(() => {
    // NOTE: as long as you mock children like below, before importing your
    // component by using a require().default pattern, this will reset your
    // mocks between tests (alex)
    jest.resetModules()
  })
  test('should return null when passed an empty query string', () => {
    const actual = getWindowPeriodVariable('', [])
    expect(actual).toEqual(null)
  })

  test('should return null when no timeRange is input', () => {
    const query = `from(bucket: "Cool Story")
    |> filter(fn: (r) => r._measurement == "cpu")
    |> filter(fn: (r) => r._field == "usage_user")`
    const actual = getWindowPeriodVariable(query, defaultVariableAssignments)
    expect(actual).toEqual(null)
  })

  test('should return a dynamic windowPeriod depending on the timeRange that is input', () => {
    jest.mock('src/external/parser', () => {
      return {
        parse: jest.fn(() => ({
          type: 'File',
          package: null,
          imports: [],
          body: [
            {
              type: 'ExpressionStatement',
              expression: {
                type: 'PipeExpression',
                argument: {type: 'PipeExpression', argument: {}, call: {}},
                call: {type: 'CallExpression', callee: {}, arguments: [{}]},
              },
            },
          ],
        })),
      }
    })
    jest.mock('src/shared/utils/getMinDurationFromAST', () => {
      return {
        getMinDurationFromAST: jest.fn(() => 86400000),
      }
    })
    const getWindowVars = require('src/variables/utils/getWindowVars')
    const query = `from(bucket: "Phil Collins")
    |> range(start: time(v: "2020-03-03T12:00:00Z"), stop: time(v: "2020-03-04T12:00:00Z"))
    |> filter(fn: (r) => r._measurement == "cpu")
    |> filter(fn: (r) => r._field == "usage_user")`

    const actual = getWindowVars.getWindowPeriodVariable(
      query,
      defaultVariableAssignments
    )
    const expected = [
      {
        orgID: '',
        id: 'windowPeriod',
        name: 'windowPeriod',
        arguments: {
          type: 'system',
          values: [240000],
        },
        status: 'Done',
        labels: [],
      },
    ]
    expect(actual).toEqual(expected)
  })
})
