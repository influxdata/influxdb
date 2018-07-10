import React, {Component} from 'react'
import Dropdown from 'src/shared/components/Dropdown'

interface Props {
  selectedTime: string
  onChooseTime: (time: string) => void
}

class TimeRangeDropdown extends Component<Props> {
  public render() {
    const {selectedTime} = this.props
    const items = [{text: 'now'}, {text: '2018-07-10T22:22:21.769Z'}]

    return (
      <Dropdown
        onChoose={this.handleChoose}
        buttonSize="btn-sm"
        buttonColor="btn-default"
        selected={selectedTime}
        items={items}
      />
    )
  }

  private handleChoose = time => {
    const {onChooseTime} = this.props
    const {text} = time

    onChooseTime(text)
  }
}

export default TimeRangeDropdown
