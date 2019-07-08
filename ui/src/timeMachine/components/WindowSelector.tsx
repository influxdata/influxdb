// Libraries
import React, {FunctionComponent} from 'react'

// Components
import {Dropdown, ComponentStatus} from '@influxdata/clockface'

// Constants
import {
  AGG_WINDOW_AUTO,
  AGG_WINDOW_NONE,
} from 'src/timeMachine/constants/queryBuilder'

interface Window {
  period: string
}

export const windows: Window[] = [
  {period: AGG_WINDOW_NONE},
  {period: AGG_WINDOW_AUTO},
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
  disabled: boolean
}

const WindowSelector: FunctionComponent<Props> = ({
  onSelect,
  period,
  disabled,
}) => {
  return (
    <Dropdown
      testID="window-selector"
      buttonTestID="window-selector--button"
      selectedID={period}
      onChange={onSelect}
      status={getStatus(disabled)}
    >
      {windows.map(({period}) => (
        <Dropdown.Item id={period} key={period} value={period} testID={period}>
          {showPrefix(period) && (
            <span className="window-selector--label">Every</span>
          )}
          {period}
        </Dropdown.Item>
      ))}
    </Dropdown>
  )
}

const showPrefix = (id: string): boolean => {
  return id !== AGG_WINDOW_AUTO && id !== AGG_WINDOW_NONE
}

const getStatus = (disabled: boolean): ComponentStatus => {
  if (disabled) {
    return ComponentStatus.Disabled
  }

  return ComponentStatus.Default
}

export default WindowSelector
