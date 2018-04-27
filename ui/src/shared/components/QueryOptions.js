import React from 'react'
import PropTypes from 'prop-types'
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

const {bool, func, shape, string} = PropTypes

QueryOptions.propTypes = {
  fill: string,
  onFill: func.isRequired,
  groupBy: shape({
    time: string,
  }).isRequired,
  shift: shape({
    label: string,
  }),
  onGroupByTime: func.isRequired,
  isKapacitorRule: bool.isRequired,
  onTimeShift: func.isRequired,
  isDisabled: bool,
}

export default QueryOptions
