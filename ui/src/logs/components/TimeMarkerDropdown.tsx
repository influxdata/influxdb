import React, {Component} from 'react'
import Dropdown from 'src/shared/components/Dropdown'
import {TimeMarker} from 'src/types/logs'

interface Props {
  onSetTimeMarker: (TimeMarker: TimeMarker) => void
  selectedTimeMarker: string
}

class TimeMarkerDropdown extends Component<Props> {
  public render() {
    const {selectedTimeMarker} = this.props
    const items = [{text: 'now'}, {text: '2018-07-10T22:22:21.769Z'}]

    return (
      <Dropdown
        onChoose={this.handleChoose}
        buttonSize="btn-sm"
        buttonColor="btn-default"
        selected={selectedTimeMarker}
        items={items}
      />
    )
  }

  private handleChoose = dropdownItem => {
    const {onSetTimeMarker} = this.props

    onSetTimeMarker({timeOption: dropdownItem.text})
  }
}

export default TimeMarkerDropdown
