// Libraries
import React, {FC, useContext} from 'react'

// Components
import {DapperScrollbars} from '@influxdata/clockface'
import SelectorListItem from 'src/notebooks/pipes/Data/SelectorListItem'
import {PipeContext} from 'src/notebooks/context/pipe'

// Constants
import {SELECTABLE_TIME_RANGES} from 'src/shared/constants/timeRanges'

const TimeSelector: FC = () => {
  const {data, update} = useContext(PipeContext)

  const timeStart = data.timeStart

  const updateTimeRange = (duration: string): void => {
    update({timeStart: duration})
  }

  return (
    <div className="data-source--block">
      <div className="data-source--block-title">Time Range</div>
      <DapperScrollbars className="data-source--list">
        {SELECTABLE_TIME_RANGES.map(range => (
          <SelectorListItem
            key={range.label}
            value={`-${range.duration}`}
            onClick={updateTimeRange}
            selected={`-${range.duration}` === timeStart}
            text={`Past ${range.duration}`}
          />
        ))}
      </DapperScrollbars>
    </div>
  )
}

export default TimeSelector
