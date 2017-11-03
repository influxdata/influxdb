import React, {PropTypes} from 'react'
import Dropdown from 'shared/components/Dropdown'
import {TIME_SHIFTS} from 'shared/constants/timeShift'

const TimeShiftDropdown = ({selected, onChooseTimeShift}) =>
  <div className="group-by-time">
    <label className="group-by-time--label">Time Shift:</label>
    <Dropdown
      className="group-by-time--dropdown"
      buttonColor="btn-info"
      items={TIME_SHIFTS}
      onChoose={onChooseTimeShift}
      selected={selected || 'none'}
    />
  </div>

const {func, string} = PropTypes

TimeShiftDropdown.propTypes = {
  selected: string,
  onChooseTimeShift: func.isRequired,
}

export default TimeShiftDropdown
