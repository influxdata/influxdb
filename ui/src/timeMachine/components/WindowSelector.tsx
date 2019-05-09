// Libraries
import React, {FunctionComponent} from 'react'

// Components
import {Dropdown} from '@influxdata/clockface'

interface Window {
  period: string
}

const windows: Window[] = [
  {period: 'auto'},
  {period: 'none'},
  {period: '5m'},
  {period: '15m'},
  {period: '1h'},
  {period: '6h'},
  {period: '12h'},
  {period: '24h'},
  {period: '2d'},
  {period: '7d'},
  {period: '30d'},
]

interface Props {
  onSelect: (period: string) => void
  period: string
}

const showPrefix = (id: string): boolean => {
  return id !== 'auto' && id !== 'none'
}

const WindowSelector: FunctionComponent<Props> = ({
  onSelect,
  period = windows[0].period,
}) => {
  return (
    <Dropdown selectedID={period} onChange={onSelect}>
      {windows.map(({period}) => (
        <Dropdown.Item id={period} key={period} value={period}>
          {showPrefix(period) && (
            <span className="window-selector--label">Every</span>
          )}
          {period}
        </Dropdown.Item>
      ))}
    </Dropdown>
  )
}

export default WindowSelector
