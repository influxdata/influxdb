import {getCheckVisTimeRange} from 'src/alerting/utils/vis'

const TESTS = [
  ['5s', {lower: 'now() - 1500s'}],
  ['1m', {lower: 'now() - 300m'}],
  ['1m5s', {lower: 'now() - 300m1500s'}],
]

test.each(TESTS)('getCheckVisTimeRange(%s)', (input, expected) => {
  expect(getCheckVisTimeRange(input)).toEqual(expected)
})
