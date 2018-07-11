import React, {Component} from 'react'
import Dropdown from 'src/shared/components/Dropdown'
import {TIME_RANGE_VALUES} from 'src/logs/constants'
import {TimeWindow} from 'src/types/logs'

interface Props {
  onSetTimeWindow: (timeWindow: TimeWindow) => void
  selectedTimeWindow: string
}

class TimeWindowDropdown extends Component<Props> {
  public render() {
    const {selectedTimeWindow} = this.props

    return (
      <Dropdown
        className="dropdown-80"
        selected={selectedTimeWindow}
        onChoose={this.handleChoose}
        buttonSize="btn-sm"
        buttonColor="btn-default"
        items={TIME_RANGE_VALUES}
      />
    )
  }

  public handleChoose = dropdownItem => {
    const {onSetTimeWindow} = this.props
    const {text, seconds} = dropdownItem

    onSetTimeWindow({seconds, windowOption: text})
  }
}

export default TimeWindowDropdown
