import {getCheckVisTimeRange} from 'src/alerting/utils/vis'
import {CHECK_TIME_RANGE} from 'src/shared/constants/timeRanges'

const TESTS = [
  ['5s', {...CHECK_TIME_RANGE, lower: 'now() - 1500s', label: 'now() - 1500s'}],
  ['1m', {...CHECK_TIME_RANGE, lower: 'now() - 300m', label: 'now() - 300m'}],
  [
    '1m5s',
    {
      ...CHECK_TIME_RANGE,
      lower: 'now() - 300m1500s',
      label: 'now() - 300m1500s',
    },
  ],
]

test.each(TESTS)('getCheckVisTimeRange(%s)', (input, expected) => {
  expect(getCheckVisTimeRange(input)).toEqual(expected)
})
