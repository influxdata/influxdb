import React from 'react'
import PropTypes from 'prop-types'
import Dropdown from 'shared/components/Dropdown'
import {TIME_SHIFTS} from 'shared/constants/timeShift'

const TimeShiftDropdown = ({selected, onChooseTimeShift, isDisabled}) => (
  <div className="group-by-time">
    <label className="group-by-time--label">Compare:</label>
    <Dropdown
      className="group-by-time--dropdown"
      buttonColor="btn-info"
      items={TIME_SHIFTS}
      onChoose={onChooseTimeShift}
      selected={selected || 'none'}
      disabled={isDisabled}
    />
  </div>
)

const {bool, func, string} = PropTypes

TimeShiftDropdown.propTypes = {
  selected: string,
  onChooseTimeShift: func.isRequired,
  isDisabled: bool,
}

export default TimeShiftDropdown
