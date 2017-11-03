import React, {PropTypes} from 'react'
import GroupByTimeDropdown from 'src/data_explorer/components/GroupByTimeDropdown'
import TimeShiftDropdown from 'src/shared/components/TimeShiftDropdown'
import FillQuery from 'shared/components/FillQuery'

const QueryOptions = ({
  fill,
  shift,
  onFill,
  groupBy,
  onTimeShift,
  onGroupByTime,
  isKapacitorRule,
}) =>
  <div className="query-builder--groupby-fill-container">
    <GroupByTimeDropdown
      selected={groupBy.time}
      onChooseGroupByTime={onGroupByTime}
    />
    <TimeShiftDropdown
      selected={shift && shift.duration}
      onChooseTimeShift={onTimeShift}
    />
    {isKapacitorRule ? null : <FillQuery value={fill} onChooseFill={onFill} />}
  </div>

const {bool, func, shape, string} = PropTypes

QueryOptions.propTypes = {
  fill: string,
  onFill: func.isRequired,
  groupBy: shape({
    time: string,
  }).isRequired,
  shift: shape({
    duration: string,
  }),
  onGroupByTime: func.isRequired,
  isKapacitorRule: bool.isRequired,
  onTimeShift: func.isRequired,
}

export default QueryOptions
