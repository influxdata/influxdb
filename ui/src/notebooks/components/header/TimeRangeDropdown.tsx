import React, {FC, useMemo} from 'react'
import {default as StatelessTimeRangeDropdown} from 'src/shared/components/TimeRangeDropdown'
import {TimeContextProps} from 'src/notebooks/components/header/Buttons'
import {TimeBlock} from 'src/notebooks/context/time'

// Utils
import {event} from 'src/notebooks/shared/event'

const TimeRangeDropdown: FC<TimeContextProps> = ({context, update}) => {
  const {range} = context

  const updateRange = range => {
    event('Time Range Updated', {
      type: range.type,
      upper: range.upper as string,
      lower: range.lower,
    })

    update({
      range,
    } as TimeBlock)
  }

  return useMemo(() => {
    return (
      <StatelessTimeRangeDropdown
        timeRange={range}
        onSetTimeRange={updateRange}
      />
    )
  }, [range])
}

export default TimeRangeDropdown
