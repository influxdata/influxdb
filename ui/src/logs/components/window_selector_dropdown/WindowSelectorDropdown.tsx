import React, {Component} from 'react'
import Dropdown from 'src/shared/components/Dropdown'
import {TIME_RANGE_VALUES} from 'src/logs/constants'
import {TimeWindow, TimeWindowOption} from 'src/types/logs'

interface Props {
  onChangeWindow: (timeWindow: TimeWindowOption) => void
  selectedTimeWindow: TimeWindow
}

class WindowSelectorDropdown extends Component<Props> {
  public render() {
    const {selectedTimeWindow, onChangeWindow} = this.props
    const {windowOption} = selectedTimeWindow

    return (
      <Dropdown
        selected={windowOption}
        onChoose={onChangeWindow}
        buttonSize="btn-sm"
        buttonColor="btn-default"
        items={TIME_RANGE_VALUES}
      />
    )
  }
}

export default WindowSelectorDropdown
