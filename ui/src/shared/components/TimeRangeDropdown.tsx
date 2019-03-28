// Libraries
import React, {PureComponent, createRef} from 'react'
import {get} from 'lodash'
import moment from 'moment'

// Components
import {Dropdown, DropdownMode} from 'src/clockface'
import DateRangePicker from 'src/shared/components/dateRangePicker/DateRangePicker'

// Constants
import {
  TIME_RANGES,
  TIME_RANGE_LABEL,
  CUSTOM_TIME_RANGE_LABEL,
  TIME_RANGE_FORMAT,
} from 'src/shared/constants/timeRanges'

// Types
import {TimeRange} from 'src/types'

interface Props {
  timeRange: TimeRange
  onSetTimeRange: (timeRange: TimeRange) => void
}

interface State {
  isDatePickerOpen: boolean
  dropdownPosition: {top: number; right: number}
}

class TimeRangeDropdown extends PureComponent<Props, State> {
  private dropdownRef = createRef<HTMLDivElement>()

  constructor(props: Props) {
    super(props)

    this.state = {isDatePickerOpen: false, dropdownPosition: undefined}
  }

  public render() {
    const timeRange = this.timeRange

    return (
      <>
        {this.isDatePickerVisible && (
          <DateRangePicker
            timeRange={timeRange}
            onSetTimeRange={this.handleApplyTimeRange}
            onClose={this.handleHideDatePicker}
            position={this.state.dropdownPosition}
          />
        )}
        <div ref={this.dropdownRef}>
          <Dropdown
            selectedID={timeRange.label}
            onChange={this.handleChange}
            widthPixels={this.dropdownWidth}
            titleText={this.formattedCustomTimeRange}
            mode={DropdownMode.ActionList}
          >
            {TIME_RANGES.map(({label}) => {
              if (label === TIME_RANGE_LABEL) {
                return <Dropdown.Divider key={label} text={label} id={label} />
              }
              return (
                <Dropdown.Item key={label} value={label} id={label}>
                  {label}
                </Dropdown.Item>
              )
            })}
          </Dropdown>
        </div>
      </>
    )
  }

  private get dropdownWidth(): number {
    if (this.isCustomTimeRange) {
      return 250
    }

    return 100
  }

  private get isCustomTimeRange(): boolean {
    const {timeRange} = this.props
    return (
      get(timeRange, 'label', '') === CUSTOM_TIME_RANGE_LABEL ||
      !!timeRange.upper
    )
  }

  private get formattedCustomTimeRange(): string {
    const {timeRange} = this.props
    if (!this.isCustomTimeRange) {
      return timeRange.label
    }

    return `${moment(timeRange.lower).format(TIME_RANGE_FORMAT)} - ${moment(
      timeRange.upper
    ).format(TIME_RANGE_FORMAT)}`
  }

  private get timeRange(): TimeRange {
    const {timeRange} = this.props
    const {isDatePickerOpen} = this.state

    if (isDatePickerOpen) {
      const date = new Date().toISOString()
      const upper =
        timeRange.upper && this.isCustomTimeRange ? timeRange.upper : date
      const lower =
        timeRange.lower && this.isCustomTimeRange ? timeRange.lower : date
      return {
        label: CUSTOM_TIME_RANGE_LABEL,
        lower,
        upper,
      }
    }

    if (this.isCustomTimeRange) {
      return {
        ...timeRange,
        label: this.formattedCustomTimeRange,
      }
    }

    const selectedTimeRange = TIME_RANGES.find(t => t.lower === timeRange.lower)

    if (!selectedTimeRange) {
      throw new Error('TimeRangeDropdown passed unknown TimeRange')
    }

    return selectedTimeRange
  }

  private get isDatePickerVisible() {
    return this.state.isDatePickerOpen
  }

  private handleApplyTimeRange = (timeRange: TimeRange) => {
    this.props.onSetTimeRange(timeRange)
    this.handleHideDatePicker()
  }

  private handleHideDatePicker = () => {
    this.setState({isDatePickerOpen: false, dropdownPosition: undefined})
  }

  private handleChange = (label: string): void => {
    const {onSetTimeRange} = this.props
    const timeRange = TIME_RANGES.find(t => t.label === label)

    if (label === CUSTOM_TIME_RANGE_LABEL) {
      const {top, left} = this.dropdownRef.current.getBoundingClientRect()
      const right = window.innerWidth - left
      this.setState({isDatePickerOpen: true, dropdownPosition: {top, right}})
      return
    }

    onSetTimeRange(timeRange)
  }
}

export default TimeRangeDropdown
