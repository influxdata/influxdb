// Libraries
import React, {FunctionComponent} from 'react'

// Components
import {
  Dropdown,
  ComponentStatus,
  DropdownItemType,
} from '@influxdata/clockface'

// Constants
import {AGG_WINDOW_AUTO} from 'src/timeMachine/constants/queryBuilder'

interface Window {
  period: string
}

export const windows: Window[] = [
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
          status={getStatus(disabled)}
          active={active}
          onClick={onClick}
          testID="window-selector--button"
        >
          {showPrefix(period) && (
            <span className="window-selector--label">Every</span>
          )}
          {period}
        </Dropdown.Button>
      )}
      menu={onCollapse => (
        <Dropdown.Menu
          onCollapse={onCollapse}
          testID="dropdown--menu window-selector"
        >
          {windows.map(w => (
            <Dropdown.Item
              onClick={onSelect}
              key={w.period}
              value={w.period}
              testID={w.period}
              type={DropdownItemType.Dot}
              selected={w.period === period}
            >
              {w.period}
            </Dropdown.Item>
          ))}
        </Dropdown.Menu>
      )}
    />
  )
}

const showPrefix = (period: string): boolean => {
  return period !== AGG_WINDOW_AUTO // && id !== AGG_WINDOW_NONE
}

const getStatus = (disabled: boolean): ComponentStatus => {
  if (disabled) {
    return ComponentStatus.Disabled
  }

  return ComponentStatus.Default
}

export default WindowSelector
