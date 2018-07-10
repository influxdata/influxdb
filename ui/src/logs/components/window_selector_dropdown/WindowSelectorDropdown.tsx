import React, {Component} from 'react'
import Dropdown from 'src/shared/components/Dropdown'
import {TIME_RANGE_VALUES} from 'src/logs/constants'

interface Props {
  onChangeWindow: (timeWindow: string) => void
  selectedTimeWindow: string
}

class WindowSelectorDropdown extends Component<Props> {
  public render() {
    const {selectedTimeWindow} = this.props

    return (
      <Dropdown
        selected={selectedTimeWindow}
        onChoose={this.handleChoose}
        buttonSize="btn-sm"
        buttonColor="btn-default"
        items={TIME_RANGE_VALUES}
      />
    )
  }

  public handleChoose = time => {
    const {onChangeWindow} = this.props
    const {text} = time

    onChangeWindow(text)
  }
}

export default WindowSelectorDropdown
