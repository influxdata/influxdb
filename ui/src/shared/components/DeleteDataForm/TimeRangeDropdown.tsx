// Libraries
import React, {useRef, useState, FunctionComponent} from 'react'
import moment from 'moment'
import {Dropdown} from '@influxdata/clockface'

// Components
import DateRangePicker from 'src/shared/components/dateRangePicker/DateRangePicker'

interface Props {
  timeRange: [number, number]
  onSetTimeRange: (timeRange: [number, number]) => any
}

const TimeRangeDropdown: FunctionComponent<Props> = ({
  timeRange,
  onSetTimeRange,
}) => {
  const [pickerActive, setPickerActive] = useState(false)
  const buttonRef = useRef<HTMLDivElement>(null)

  let datePickerPosition = null

  if (buttonRef.current) {
    const {right, top} = buttonRef.current.getBoundingClientRect()

    datePickerPosition = {top: top, right: window.innerWidth - right}
  }

  const lower = moment(timeRange[0]).format('YYYY-MM-DD HH:mm:ss')
  const upper = moment(timeRange[1]).format('YYYY-MM-DD HH:mm:ss')

  return (
    <div ref={buttonRef}>
      <Dropdown.Button onClick={() => setPickerActive(!pickerActive)}>
        {lower} - {upper}
      </Dropdown.Button>
      {pickerActive && (
        <DateRangePicker
          timeRange={{lower, upper}}
          onSetTimeRange={({lower, upper}) =>
            onSetTimeRange([Date.parse(lower), Date.parse(upper)])
          }
          position={datePickerPosition}
          onClose={() => setPickerActive(false)}
        />
      )}
    </div>
  )
}

export default TimeRangeDropdown
