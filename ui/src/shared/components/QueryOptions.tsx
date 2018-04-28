import React, {SFC} from 'react'

import {GroupBy, TimeShift} from 'src/types'

import GroupByTimeDropdown from 'src/data_explorer/components/GroupByTimeDropdown'
import TimeShiftDropdown from 'src/shared/components/TimeShiftDropdown'
import FillQuery from 'src/shared/components/FillQuery'

interface Props {
  fill: string
  onFill: () => void
  groupBy: GroupBy
  shift: TimeShift
  onGroupByTime: () => void
  isKapacitorRule: boolean
  onTimeShift: () => void
  isDisabled: boolean
}

const QueryOptions: SFC<Props> = ({
  fill,
  shift,
  onFill,
  groupBy,
  onTimeShift,
  onGroupByTime,
  isKapacitorRule,
  isDisabled,
}) => (
  <div className="query-builder--groupby-fill-container">
    <GroupByTimeDropdown
      selected={groupBy.time}
      onChooseGroupByTime={onGroupByTime}
      isDisabled={isDisabled}
    />
    {isKapacitorRule ? null : (
      <TimeShiftDropdown
        selected={shift && shift.label}
        onChooseTimeShift={onTimeShift}
        isDisabled={isDisabled}
      />
    )}
    {isKapacitorRule ? null : (
      <FillQuery value={fill} onChooseFill={onFill} isDisabled={isDisabled} />
    )}
  </div>
)

export default QueryOptions
