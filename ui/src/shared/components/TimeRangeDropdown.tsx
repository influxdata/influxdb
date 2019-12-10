// Libraries
import React, {PureComponent, createRef} from 'react'

// Components
import {
  Dropdown,
  Popover,
  PopoverPosition,
  PopoverInteraction,
  Appearance,
} from '@influxdata/clockface'
import DateRangePicker from 'src/shared/components/dateRangePicker/DateRangePicker'

// Utils
import {
  convertTimeRangeToCustom,
  getTimeRangeLabel,
} from 'src/shared/utils/duration'

// Constants
import {
  SELECTABLE_TIME_RANGES,
  CUSTOM_TIME_RANGE_LABEL,
} from 'src/shared/constants/timeRanges'

// Types
import {
  TimeRange,
  CustomTimeRange,
  SelectableDurationTimeRange,
} from 'src/types'

interface Props {
  timeRange: TimeRange
  onSetTimeRange: (timeRange: TimeRange) => void
}

interface State {
  isDatePickerOpen: boolean
}

class TimeRangeDropdown extends PureComponent<Props, State> {
  private dropdownRef = createRef<HTMLDivElement>()

  constructor(props: Props) {
    super(props)

    this.state = {isDatePickerOpen: false}
  }

  public render() {
    const timeRange = this.timeRange
    const timeRangeLabel = getTimeRangeLabel(timeRange)
    return (
      <>
        <Popover
          appearance={Appearance.Outline}
          position={PopoverPosition.ToTheLeft}
          triggerRef={this.dropdownRef}
          visible={this.state.isDatePickerOpen}
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
              position={
                this.state.isDatePickerOpen ? {position: 'relative'} : undefined
              }
            />
          )}
        />
        <div ref={this.dropdownRef}>
          <Dropdown
            style={{width: `${this.dropdownWidth}px`}}
            testID="timerange-dropdown"
            button={(active, onClick) => (
              <Dropdown.Button active={active} onClick={onClick}>
                {timeRangeLabel}
              </Dropdown.Button>
            )}
            menu={onCollapse => (
              <Dropdown.Menu
                onCollapse={onCollapse}
                style={{width: `${this.dropdownWidth + 50}px`}}
              >
                <Dropdown.Divider
                  key="Time Range"
                  text="Time Range"
                  id="Time Range"
                />
                <Dropdown.Item
                  key={CUSTOM_TIME_RANGE_LABEL}
                  value={CUSTOM_TIME_RANGE_LABEL}
                  id={CUSTOM_TIME_RANGE_LABEL}
                  testID="dropdown-item-customtimerange"
                  selected={this.state.isDatePickerOpen}
                  onClick={this.handleClickCustomTimeRange}
                >
                  {CUSTOM_TIME_RANGE_LABEL}
                </Dropdown.Item>
                {SELECTABLE_TIME_RANGES.map(({label}) => {
                  const testID = label.toLowerCase().replace(/\s/g, '')
                  return (
                    <Dropdown.Item
                      key={label}
                      value={label}
                      id={label}
                      testID={`dropdown-item-${testID}`}
                      selected={label === timeRangeLabel}
                      onClick={this.handleClickDropdownItem}
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
    if (this.props.timeRange.type === 'custom') {
      return 250
    }
    return 100
  }

  private get timeRange(): CustomTimeRange | SelectableDurationTimeRange {
    const {timeRange} = this.props
    const {isDatePickerOpen} = this.state

    if (isDatePickerOpen && timeRange.type !== 'custom') {
      return convertTimeRangeToCustom(timeRange)
    }

    if (timeRange.type === 'duration') {
      return convertTimeRangeToCustom(timeRange)
    }

    return timeRange
  }

  private handleApplyTimeRange = (timeRange: TimeRange) => {
    this.props.onSetTimeRange(timeRange)
    this.handleHideDatePicker()
  }

  private handleHideDatePicker = () => {
    this.setState({isDatePickerOpen: false})
  }

  private handleClickCustomTimeRange = (): void => {
    this.setState({isDatePickerOpen: true})
  }

  private handleClickDropdownItem = (label: string): void => {
    const {onSetTimeRange} = this.props
    const timeRange = SELECTABLE_TIME_RANGES.find(t => t.label === label)

    onSetTimeRange(timeRange)
  }
}

export default TimeRangeDropdown
