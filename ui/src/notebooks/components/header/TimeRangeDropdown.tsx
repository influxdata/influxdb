import React, {FC, useMemo, useCallback} from 'react'
import {default as StatelessTimeRangeDropdown} from 'src/shared/components/TimeRangeDropdown'
import {TimeContextProps} from 'src/notebooks/components/header'
import {TimeBlock} from 'src/notebooks/context/time'

// Utils
import {event} from 'src/cloud/utils/reporting'

const TimeRangeDropdown: FC<TimeContextProps> = ({context, update}) => {
  const {range} = context

  const updateRange = useCallback(
    range => {
      event('Time Range Updated', {
        type: range.type,
        upper: range.upper as string,
        lower: range.lower,
      })

      update({
        range,
      } as TimeBlock)
    },
    [update]
  )

  return useMemo(() => {
    return (
      <StatelessTimeRangeDropdown
        timeRange={range}
        onSetTimeRange={updateRange}
      />
    )
  }, [range, updateRange])
}

export default TimeRangeDropdown
