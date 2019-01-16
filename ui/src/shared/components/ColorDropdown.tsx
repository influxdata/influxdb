// Libraries
import React, {SFC} from 'react'

// Components
import {Dropdown, ComponentStatus, DropdownMenuColors} from 'src/clockface'

// Types
import {ColorLabel} from 'src/types/colors'

interface Props {
  selected: ColorLabel
  disabled?: boolean
  stretchToFit?: boolean
  colors: ColorLabel[]
  onChoose: (colors: ColorLabel) => void
}

const titleCase = (name: string) => `${name[0].toUpperCase()}${name.slice(1)}`

const ColorDropdown: SFC<Props> = props => {
  const {selected, colors, onChoose, disabled, stretchToFit} = props

  const status = disabled ? ComponentStatus.Disabled : ComponentStatus.Default
  const widthPixels = stretchToFit ? null : 200

  return (
    <Dropdown
      selectedID={selected.name}
      onChange={onChoose}
      status={status}
      widthPixels={widthPixels}
      menuColor={DropdownMenuColors.Onyx}
    >
      {colors.map(color => (
        <Dropdown.Item id={color.name} key={color.name} value={color}>
          <div className="color-dropdown--item">
            <div
              className="color-dropdown--swatch"
              style={{backgroundColor: color.hex}}
            />
            <div className="color-dropdown--name">{titleCase(color.name)}</div>
          </div>
        </Dropdown.Item>
      ))}
    </Dropdown>
  )
}

export default ColorDropdown
