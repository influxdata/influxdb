import React, {Component} from 'react'
import Dropdown from 'src/shared/components/Dropdown'
import {TIME_RANGE_VALUES} from 'src/logs/constants'
import {TimeWindow, TimeRange} from 'src/types/logs'

interface Props {
  onSetTimeWindow: (timeWindow: TimeWindow) => void
  selectedTimeWindow: TimeRange
}

class TimeWindowDropdown extends Component<Props> {
  public render() {
    return (
      <Dropdown
        className="dropdown-90"
        selected={this.selected}
        onChoose={this.handleChoose}
        buttonSize="btn-sm"
        buttonColor="btn-default"
        items={TIME_RANGE_VALUES}
      />
    )
  }

  private get selected(): string {
    const {
      selectedTimeWindow: {timeOption, windowOption},
    } = this.props

    if (timeOption === 'now') {
      return `- ${windowOption}`
    }

    return `+/- ${windowOption}`
  }

  private handleChoose = (dropdownItem): void => {
    const {onSetTimeWindow} = this.props
    const {text, seconds} = dropdownItem

    onSetTimeWindow({seconds, windowOption: text})
  }
}

export default TimeWindowDropdown
