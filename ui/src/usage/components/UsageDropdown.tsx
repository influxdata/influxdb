import React, {Component} from 'react'

import {SelectDropdown, ComponentColor} from '@influxdata/clockface'

const ranges = ['Writes (MB)', 'Total Query Duration (s)', 'Storage (GB-hr)']

interface Props {
  selectedUsage: string
  onSelect: (s: string) => void
}

class UsageDropdown extends Component<Props> {
  constructor(props) {
    super(props)
  }

  render() {
    const {selectedUsage} = this.props

    return (
      <SelectDropdown
        selectedOption={selectedUsage}
        options={ranges}
        onSelect={this.handleSelect}
        buttonColor={ComponentColor.Default}
        style={{width: '200px'}}
      />
    )
  }

  handleSelect = v => {
    const {onSelect} = this.props

    this.setState({selectedUsage: v}, () => {
      if (onSelect) {
        onSelect(v)
      }
    })
  }
}

export default UsageDropdown
