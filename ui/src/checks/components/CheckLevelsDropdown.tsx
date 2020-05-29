// Libraries
import React, {FC} from 'react'

// Components
import {
  Dropdown,
  ComponentColor,
  InfluxColors,
  DropdownMenuTheme,
} from '@influxdata/clockface'

// Types
import {CheckStatusLevel} from 'src/types'

type ColorLevel = {hex: InfluxColors; display: string; value: CheckStatusLevel}

const levels: ColorLevel[] = [
  {display: 'CRIT', hex: InfluxColors.Fire, value: 'CRIT'},
  {display: 'INFO', hex: InfluxColors.Ocean, value: 'INFO'},
  {display: 'WARN', hex: InfluxColors.Thunder, value: 'WARN'},
  {display: 'OK', hex: InfluxColors.Viridian, value: 'OK'},
]

interface Props {
  selectedLevel: CheckStatusLevel
  onClickLevel: (level: CheckStatusLevel) => void
}

const CheckLevelsDropdown: FC<Props> = ({selectedLevel, onClickLevel}) => {
  const selected = levels.find(l => l.value === selectedLevel)

  if (!selected) {
    throw new Error('Unknown level type provided to <CheckLevelsDropdown/>')
  }

  const button = (active: boolean, onClick) => (
    <Dropdown.Button
      color={ComponentColor.Default}
      active={active}
      onClick={onClick}
      testID="check-levels--dropdown--button"
    >
      <div className="color-dropdown--item">
        <div
          className="color-dropdown--swatch"
          style={{backgroundColor: selected.hex}}
        />
        <div className="color-dropdown--name">{selected.value}</div>
      </div>
    </Dropdown.Button>
  )

  const items = levels.map(({value, display, hex}) => (
    <Dropdown.Item
      key={value}
      id={value}
      value={value}
      onClick={() => onClickLevel(value)}
      testID={`check-levels--dropdown-item ${value}`}
    >
      <div className="color-dropdown--item">
        <div
          className="color-dropdown--swatch"
          style={{backgroundColor: hex}}
        />
        <div className="color-dropdown--name">{display}</div>
      </div>
    </Dropdown.Item>
  ))

  const menu = (onCollapse: () => void) => (
    <Dropdown.Menu theme={DropdownMenuTheme.Onyx} onCollapse={onCollapse}>
      {items}
    </Dropdown.Menu>
  )

  return (
    <Dropdown button={button} menu={menu} testID="check-levels--dropdown" />
  )
}

export default CheckLevelsDropdown
