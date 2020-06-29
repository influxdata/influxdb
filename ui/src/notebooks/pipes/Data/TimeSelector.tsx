// Libraries
import React, {FC} from 'react'

// Components
import {DapperScrollbars} from '@influxdata/clockface'
import SelectorListItem from 'src/notebooks/pipes/Data/SelectorListItem'

// Types
import {PipeData} from 'src/notebooks'

// Constants
import {SELECTABLE_TIME_RANGES} from 'src/shared/constants/timeRanges'

interface Props {
  onUpdate: (data: any) => void
  data: PipeData
}

const TimeSelector: FC<Props> = ({onUpdate, data}) => {
  const timeStart = data.timeStart

  const updateTimeRange = (duration: string): void => {
    onUpdate({timeStart: duration})
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
