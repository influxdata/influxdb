import {getCheckVisTimeRange} from 'src/alerting/utils/vis'

const duration = 'duration' as 'duration'
const TESTS = [
  ['5s', {type: duration, lower: 'now() - 500s', upper: null}],
  ['1m', {type: duration, lower: 'now() - 100m', upper: null}],
  [
    '1m5s',
    {
      type: duration,
      lower: 'now() - 100m500s',
      upper: null,
    },
  ],
]

test.each(TESTS)('getCheckVisTimeRange(%s)', (input, expected) => {
  expect(getCheckVisTimeRange(input)).toEqual(expected)
})
