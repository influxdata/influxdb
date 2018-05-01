import React, {SFC} from 'react'

import Dropdown from 'src/shared/components/Dropdown'
import {TIME_SHIFTS} from 'src/shared/constants/timeShift'
import {TimeShift} from 'src/types'

interface Props {
  selected: string
  onChooseTimeShift: (timeShift: TimeShift) => void
  isDisabled: boolean
}

const TimeShiftDropdown: SFC<Props> = ({
  selected,
  onChooseTimeShift,
  isDisabled,
}) => (
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

export default TimeShiftDropdown
