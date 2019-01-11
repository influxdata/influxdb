// Libraries
import React, {PureComponent} from 'react'

// Components
import {Dropdown} from 'src/clockface'

// Constants
import {TIME_RANGES} from 'src/shared/constants/timeRanges'

// Types
import {TimeRange} from 'src/types'

interface Props {
  timeRange: TimeRange
  onSetTimeRange: (timeRange: TimeRange) => void
}

class TimeRangeDropdown extends PureComponent<Props> {
  public render() {
    const {timeRange} = this.props
    const selectedTimeRange = TIME_RANGES.find(t => t.lower === timeRange.lower)

    if (!selectedTimeRange) {
      throw new Error('TimeRangeDropdown passed unknown TimeRange')
    }

    return (
      <Dropdown
        selectedID={selectedTimeRange.label}
        onChange={this.handleChange}
        widthPixels={100}
      >
        {TIME_RANGES.map(({label}) => (
          <Dropdown.Item key={label} value={label} id={label}>
            {label}
          </Dropdown.Item>
        ))}
      </Dropdown>
    )
  }

  private handleChange = (label: string): void => {
    const {onSetTimeRange} = this.props
    const timeRange = TIME_RANGES.find(t => t.label === label)

    onSetTimeRange(timeRange)
  }
}

export default TimeRangeDropdown
