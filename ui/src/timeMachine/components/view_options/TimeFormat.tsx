// Libraries
import React, {FunctionComponent} from 'react'
import {SelectDropdown} from '@influxdata/clockface'

// Utils
import {resolveTimeFormat} from 'src/dashboards/utils/tableGraph'

// Constants
import {FORMAT_OPTIONS} from 'src/dashboards/constants'

interface Props {
  timeFormat: string
  onTimeFormatChange: (format: string) => void
}

const TimeFormatSetting: FunctionComponent<Props> = ({
  timeFormat,
  onTimeFormatChange,
}) => (
  <SelectDropdown
    options={FORMAT_OPTIONS.map(option => option.text)}
    selectedOption={resolveTimeFormat(timeFormat)}
    onSelect={onTimeFormatChange}
  />
)

export default TimeFormatSetting
