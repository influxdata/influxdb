// Libraries
import React, {SFC} from 'react'

// Components
import {Dropdown, DropdownMenuColors} from 'src/clockface'
import ColorSchemeDropdownItem from 'src/shared/components/ColorSchemeDropdownItem'

// Constants
import {
  LINE_COLOR_SCALES,
  DEFAULT_LINE_COLORS,
} from 'src/shared/constants/graphColorPalettes'

// Types
import {Color} from 'src/types/colors'

interface Props {
  value: Color[]
  onChange: (colors: Color[]) => void
}

const findSelectedScaleID = (colors: Color[]) => {
  const key = (colors: Color[]) => colors.map(color => color.hex).join(', ')
  const needle = key(colors)
  const selectedScale = LINE_COLOR_SCALES.find(d => key(d.colors) === needle)

  if (selectedScale) {
    return selectedScale.id
  } else {
    return DEFAULT_LINE_COLORS[0].id
  }
}

const ColorSchemeDropdown: SFC<Props> = ({value, onChange}) => {
  return (
    <Dropdown
      selectedID={findSelectedScaleID(value)}
      onChange={onChange}
      menuColor={DropdownMenuColors.Onyx}
    >
      {LINE_COLOR_SCALES.map(({id, name, colors}) => (
        <Dropdown.Item key={id} id={id} value={colors}>
          <ColorSchemeDropdownItem
            name={name}
            colors={colors.map(c => c.hex)}
          />
        </Dropdown.Item>
      ))}
    </Dropdown>
  )
}

export default ColorSchemeDropdown
