// Libraries
import React, {SFC} from 'react'

// Components
import {Dropdown, ComponentStatus, DropdownMenuColors} from 'src/clockface'

// Types
import {ColorLabel} from 'src/types/colors'

interface PassedProps {
  selected: ColorLabel
  colors: ColorLabel[]
  onChoose: (colors: ColorLabel) => void
}

interface DefaultProps {
  disabled?: boolean
  stretchToFit?: boolean
  widthPixels?: number
}

type Props = PassedProps & DefaultProps

const titleCase = (name: string) => `${name[0].toUpperCase()}${name.slice(1)}`

const ColorDropdown: SFC<Props> = props => {
  const {
    selected,
    colors,
    onChoose,
    disabled,
    stretchToFit,
    widthPixels,
  } = props

  const status = disabled ? ComponentStatus.Disabled : ComponentStatus.Default
  const width = stretchToFit ? null : widthPixels

  return (
    <Dropdown
      selectedID={selected.name}
      onChange={onChoose}
      status={status}
      widthPixels={width}
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

ColorDropdown.defaultProps = {
  stretchToFit: false,
  disabled: false,
  widthPixels: 100,
}

export default ColorDropdown
