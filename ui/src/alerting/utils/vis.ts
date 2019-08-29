// Utils
import {parseDuration} from 'src/variables/utils/parseDuration'

// Types
import {TimeRange} from 'src/types'

const POINTS_PER_CHECK_PLOT = 300

/*
  Given the duration in a check's `every` field, return a `TimeRange` suitable
  for visualizing the input data to the check.

  For example, suppose a check has an `every` value of `1m`. Then the check is
  run once per minute, and the input data to the `check` for each run is a
  single value aggregated from the last `1m`. To display a visualization of the
  check data over time, we want to show a series of the values aggregated for
  each minute. So to display a plot with say, 300 points, we need to query a
  time range of the last 300 minutes.
*/
export const getCheckVisTimeRange = (durationStr: string): TimeRange => {
  const durationMultiple = parseDuration(durationStr)
    .map(({magnitude, unit}) => `${magnitude * POINTS_PER_CHECK_PLOT}${unit}`)
    .join('')

  return {lower: `now() - ${durationMultiple}`}
}
