// Libraries
import React, {useRef, useState, FC} from 'react'
import moment from 'moment'

// Components
import {
  Dropdown,
  Popover,
  PopoverPosition,
  PopoverInteraction,
  Appearance,
} from '@influxdata/clockface'
import DateRangePicker from 'src/shared/components/dateRangePicker/DateRangePicker'

// Types
import {CustomTimeRange} from 'src/types'
import {
  TIME_RANGE_FORMAT,
  pastHourTimeRange,
} from 'src/shared/constants/timeRanges'
import {convertTimeRangeToCustom} from 'src/shared/utils/duration'

interface Props {
  timeRange: CustomTimeRange
  onSetTimeRange: (timeRange: CustomTimeRange) => void
}

const TimeRangeDropdown: FC<Props> = ({timeRange, onSetTimeRange}) => {
  const [pickerActive, setPickerActive] = useState(false)
  const buttonRef = useRef<HTMLDivElement>(null)

  let dropdownLabel = 'Select a Time Range'

  if (timeRange) {
    dropdownLabel = `${moment(timeRange.lower).format(
      TIME_RANGE_FORMAT
    )} - ${moment(timeRange.upper).format(TIME_RANGE_FORMAT)}`
  }

  const handleApplyTimeRange = (timeRange: CustomTimeRange) => {
    onSetTimeRange(timeRange)
    setPickerActive(false)
  }

  return (
    <div ref={buttonRef}>
      <Dropdown.Button onClick={() => setPickerActive(!pickerActive)}>
        {dropdownLabel}
      </Dropdown.Button>
      <Popover
        appearance={Appearance.Outline}
        position={PopoverPosition.Below}
        triggerRef={buttonRef}
        visible={pickerActive}
        showEvent={PopoverInteraction.None}
        hideEvent={PopoverInteraction.None}
        distanceFromTrigger={8}
        testID="timerange-popover"
        enableDefaultStyles={false}
        contents={() => (
          <DateRangePicker
            timeRange={timeRange || convertTimeRangeToCustom(pastHourTimeRange)}
            onSetTimeRange={handleApplyTimeRange}
            onClose={() => setPickerActive(false)}
            position={{position: 'relative'}}
          />
        )}
      />
    </div>
  )
}

export default TimeRangeDropdown
