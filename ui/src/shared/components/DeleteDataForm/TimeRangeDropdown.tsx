// Libraries
import React, {useRef, useState, FC} from 'react'
import moment from 'moment'
import {
  Dropdown,
  Popover,
  PopoverPosition,
  PopoverInteraction,
  Appearance,
} from '@influxdata/clockface'

// Components
import DateRangePicker from 'src/shared/components/dateRangePicker/DateRangePicker'

interface Props {
  timeRange: [number, number]
  onSetTimeRange: (timeRange: [number, number]) => any
}

const TimeRangeDropdown: FC<Props> = ({timeRange, onSetTimeRange}) => {
  const [pickerActive, setPickerActive] = useState(false)
  const buttonRef = useRef<HTMLDivElement>(null)

  const lower = moment(timeRange[0]).format('YYYY-MM-DD HH:mm:ss')
  const upper = moment(timeRange[1]).format('YYYY-MM-DD HH:mm:ss')

  const handleApplyTimeRange = (lower, upper) => {
    onSetTimeRange([Date.parse(lower), Date.parse(upper)])
    setPickerActive(false)
  }
  return (
    <div ref={buttonRef}>
      <Dropdown.Button onClick={() => setPickerActive(!pickerActive)}>
        {lower} - {upper}
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
            timeRange={{lower, upper}}
            onSetTimeRange={({lower, upper}) =>
              handleApplyTimeRange(lower, upper)
            }
            onClose={() => setPickerActive(false)}
            position={{position: 'relative'}}
          />
        )}
      />
    </div>
  )
}

export default TimeRangeDropdown
