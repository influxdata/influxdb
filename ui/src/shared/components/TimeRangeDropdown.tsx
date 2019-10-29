// Libraries
import React, {PureComponent, createRef} from 'react'
import {get} from 'lodash'
import moment from 'moment'

// Components
import {
  Dropdown,
  Popover,
  PopoverPosition,
  PopoverInteraction,
  PopoverType,
} from '@influxdata/clockface'
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

export enum RangeType {
  Absolute = 'absolute',
  Relative = 'relative',
}

interface Props {
  timeRange: TimeRange
  onSetTimeRange: (timeRange: TimeRange, rangeType?: RangeType) => void
}

interface State {
  isDatePickerOpen: boolean
  dropdownPosition: {position: string}
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
        <Popover
          type={PopoverType.Outline}
          position={PopoverPosition.ToTheLeft}
          triggerRef={this.dropdownRef}
          visible={this.isDatePickerVisible}
          showEvent={PopoverInteraction.None}
          hideEvent={PopoverInteraction.None}
          distanceFromTrigger={8}
          testID="timerange-popover"
          enableDefaultStyles={false}
          contents={() => (
            <DateRangePicker
              timeRange={timeRange}
              onSetTimeRange={this.handleApplyTimeRange}
              onClose={this.handleHideDatePicker}
              position={this.state.dropdownPosition}
            />
          )}
        />
        <div ref={this.dropdownRef}>
          <Dropdown
            style={{width: `${this.dropdownWidth}px`}}
            testID="timerange-dropdown"
            button={(active, onClick) => (
              <Dropdown.Button active={active} onClick={onClick}>
                {this.formattedCustomTimeRange}
              </Dropdown.Button>
            )}
            menu={onCollapse => (
              <Dropdown.Menu
                onCollapse={onCollapse}
                style={{width: `${this.dropdownWidth + 50}px`}}
              >
                {TIME_RANGES.map(({label}) => {
                  if (label === TIME_RANGE_LABEL) {
                    return (
                      <Dropdown.Divider key={label} text={label} id={label} />
                    )
                  }
                  const testID = label.toLowerCase().replace(/\s/g, '')
                  return (
                    <Dropdown.Item
                      key={label}
                      value={label}
                      id={label}
                      testID={`dropdown-item-${testID}`}
                      selected={label === timeRange.label}
                      onClick={this.handleChange}
                    >
                      {label}
                    </Dropdown.Item>
                  )
                })}
              </Dropdown.Menu>
            )}
          />
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
      return TIME_RANGES.find(range => range.lower === timeRange.lower).label
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
        timeRange.lower && this.isCustomTimeRange
          ? timeRange.lower
          : this.calculatedLower
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

  private get calculatedLower() {
    const {
      timeRange: {seconds},
    } = this.props

    if (seconds) {
      return moment()
        .subtract(seconds, 's')
        .toISOString()
    }

    return new Date().toISOString()
  }

  private handleApplyTimeRange = (timeRange: TimeRange) => {
    this.props.onSetTimeRange(timeRange, RangeType.Absolute)
    this.handleHideDatePicker()
  }

  private handleHideDatePicker = () => {
    this.setState({isDatePickerOpen: false, dropdownPosition: undefined})
  }

  private handleChange = (label: string): void => {
    const {onSetTimeRange} = this.props
    const timeRange = TIME_RANGES.find(t => t.label === label)

    if (label === CUSTOM_TIME_RANGE_LABEL) {
      this.setState({
        isDatePickerOpen: true,
        dropdownPosition: {position: 'relative'},
      })
      return
    }

    onSetTimeRange(timeRange)
  }
}

export default TimeRangeDropdown
