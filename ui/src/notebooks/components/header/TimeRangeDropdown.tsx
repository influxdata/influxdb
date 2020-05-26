import React, {FC, useMemo} from 'react'
import {default as StatelessTimeRangeDropdown} from 'src/shared/components/TimeRangeDropdown'


const TimeRangeDropdown:FC = ({context, update}) => {
  const {range} = context

  const updateRange = range => {
    update({
      range,
    })
  }

  return useMemo(() => {
    return <StatelessTimeRangeDropdown timeRange={range} onSetTimeRange={updateRange} />
  }, [range])
}

export default TimeRangeDropdown
