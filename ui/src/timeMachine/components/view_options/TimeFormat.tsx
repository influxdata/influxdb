// Libraries
import React, {FunctionComponent} from 'react'
import {Dropdown} from '@influxdata/clockface'

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
  <Dropdown
    selectedID={resolveTimeFormat(timeFormat)}
    onChange={onTimeFormatChange}
  >
    {FORMAT_OPTIONS.map(({text}) => (
      <Dropdown.Item key={text} id={text} value={text}>
        {text}
      </Dropdown.Item>
    ))}
  </Dropdown>
)

export default TimeFormatSetting
