import React, {SFC} from 'react'

import {GroupBy, TimeShift} from 'src/types'

import GroupByTimeDropdown from 'src/shared/components/GroupByTimeDropdown'
import TimeShiftDropdown from 'src/shared/components/TimeShiftDropdown'
import FillQuery from 'src/shared/components/FillQuery'

interface Props {
  fill: string
  onFill: (fill: string) => void
  groupBy: GroupBy
  shift: TimeShift
  onGroupByTime: (groupBy: GroupBy) => void
  onTimeShift: (shift: TimeShift) => void
  isDisabled: boolean
}

const QueryOptions: SFC<Props> = ({
  fill,
  shift,
  onFill,
  groupBy,
  onTimeShift,
  onGroupByTime,
  isDisabled,
}) => (
  <div className="query-builder--groupby-fill-container">
    <GroupByTimeDropdown
      selected={groupBy.time}
      onChooseGroupByTime={onGroupByTime}
      isDisabled={isDisabled}
    />
    <TimeShiftDropdown
      selected={shift && shift.label}
      onChooseTimeShift={onTimeShift}
      isDisabled={isDisabled}
    />
    <FillQuery value={fill} onChooseFill={onFill} isDisabled={isDisabled} />
    )}
  </div>
)

export default QueryOptions
