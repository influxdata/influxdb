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
      button={(active, onClick) => (
        <Dropdown.Button
          testID="window-selector--button"
          active={active}
          onClick={onClick}
          status={getStatus(disabled)}
        >
          {showPrefix(period) && (
            <span className="window-selector--label">Every</span>
          )}
          {period}
        </Dropdown.Button>
      )}
      menu={onCollapse => (
        <Dropdown.Menu onCollapse={onCollapse} testID="window-selector--menu">
          {windows.map(window => (
            <Dropdown.Item
              id={window.period}
              key={window.period}
              value={window.period}
              testID={`window-selector--${window.period}`}
              selected={window.period === period}
              onClick={onSelect}
            >
              {showPrefix(window.period) && (
                <span className="window-selector--label">Every</span>
              )}
              {window.period}
            </Dropdown.Item>
          ))}
        </Dropdown.Menu>
      )}
    />
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
