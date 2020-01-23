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
import {RuleStatusLevel} from 'src/types'

type LevelType = 'currentLevel' | 'previousLevel'
type ColorLevel = {hex: InfluxColors; display: string; value: RuleStatusLevel}

interface Props {
  selectedLevel: RuleStatusLevel
  otherLevel: RuleStatusLevel
  type: LevelType
  onClickLevel: (type: LevelType, level: RuleStatusLevel) => void
}

const RuleLevelsDropdown: FC<Props> = ({
  type,
  selectedLevel,
  otherLevel,
  onClickLevel,
}) => {
  let levels: ColorLevel[] = [
    {display: 'CRIT', hex: InfluxColors.Fire, value: 'CRIT'},
    {display: 'INFO', hex: InfluxColors.Ocean, value: 'INFO'},
    {display: 'WARN', hex: InfluxColors.Thunder, value: 'WARN'},
    {display: 'OK', hex: InfluxColors.Viridian, value: 'OK'},
    {display: 'ANY', hex: InfluxColors.Sidewalk, value: 'ANY'},
  ]
  if (otherLevel) {
    levels = levels.filter(l => l.value !== otherLevel)
  }

  const selected = levels.find(l => l.value === selectedLevel)

  if (!selected) {
    throw new Error('Unknown level type provided to <LevelsDropdown/>')
  }

  const button = (active, onClick) => (
    <Dropdown.Button
      color={ComponentColor.Default}
      active={active}
      onClick={onClick}
      testID={`levels--dropdown--button ${type}`}
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
      onClick={() => onClickLevel(type, value)}
      testID={`levels--dropdown-item ${value}`}
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

  const menu = onCollapse => (
    <Dropdown.Menu theme={DropdownMenuTheme.Onyx} onCollapse={onCollapse}>
      {items}
    </Dropdown.Menu>
  )

  return (
    <Dropdown button={button} menu={menu} testID={`levels--dropdown ${type}`} />
  )
}

export default RuleLevelsDropdown
