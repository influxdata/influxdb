import React, {Component} from 'react'

import {Form, Dropdown, ComponentColor} from '@influxdata/clockface'

import {UsageRanges} from 'src/types'

interface Props {
  selectedTimeRange: string
  dropdownOptions: UsageRanges
  onSelect: (range: string) => void
}

class TimeRangeDropdown extends Component<Props> {
  render() {
    const {selectedTimeRange} = this.props

    return (
      <form>
        <Form.Element label="Time Range">
          <Dropdown
            testID="timerange-dropdown"
            style={{width: '200px'}}
            button={(active, onClick) => (
              <Dropdown.Button
                active={active}
                onClick={onClick}
                color={ComponentColor.Primary}
              >
                {selectedTimeRange}
              </Dropdown.Button>
            )}
            menu={() => <Dropdown.Menu>{this.dropdownOptions()}</Dropdown.Menu>}
          />
        </Form.Element>
      </form>
    )
  }

  dropdownOptions() {
    const {dropdownOptions} = this.props
    return Object.keys(dropdownOptions).map(opt => (
      <Dropdown.Item
        testID={`timerange-dropdown--${opt}`}
        key={`timerange-dropdown--${opt}`}
        value={opt}
        onClick={() => console.log('clicking time range')}
      >
        {dropdownOptions[opt]}
      </Dropdown.Item>
    ))
  }
}

export default TimeRangeDropdown
