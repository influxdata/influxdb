// Libraries
import {NumericColumnData} from '@influxdata/giraffe'
import {useMemo} from 'react'

// Utils
import {parseDuration} from 'src/shared/utils/duration'
import {useOneWayState} from 'src/shared/utils/useOneWayState'
import {extent} from 'src/shared/utils/vis'
import {flatMap} from 'lodash'

// Types
import {Threshold, DurationTimeRange} from 'src/types'

const POINTS_PER_CHECK_PLOT = 100

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
export const getCheckVisTimeRange = (
  durationStr: string
): DurationTimeRange => {
  const durationMultiple = parseDuration(durationStr)
    .map(({magnitude, unit}) => `${magnitude * POINTS_PER_CHECK_PLOT}${unit}`)
    .join('')

  const lower = `now() - ${durationMultiple}`

  return {upper: null, lower, type: 'duration'}
}

/*
  Obtain the y domain settings for a threshold check plot.

  The y domain for a threshold check plot should be large enough to show every
  threshold value in addition to every y value in the plot.
*/
export const useCheckYDomain = (
  data: NumericColumnData,
  thresholds: Threshold[]
) => {
  const dataDomain = useMemo(() => extent(data as number[]), [data])

  const initialDomain: number[] = useMemo(() => {
    const extrema: number[] = flatMap(thresholds || [], (t: any) => [
      t.value,
      t.min,
      t.max,
    ])
      .filter(v => v !== undefined && v !== null)
      .concat(dataDomain)

    return extent(extrema)
  }, [dataDomain, thresholds])

  const [domain, setDomain] = useOneWayState(initialDomain)
  const resetDomain = () => setDomain(initialDomain)

  return [domain, setDomain, resetDomain]
}
