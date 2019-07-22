// Libraries
import React, {SFC} from 'react'

// Components
import {Dropdown, DropdownMenuTheme} from '@influxdata/clockface'
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
      button={(active, onClick) => (
        <Dropdown.Button active={active} onClick={onClick}>
          <ColorSchemeDropdownItem
            name={value[0].name}
            colors={value.map(c => c.hex)}
          />
        </Dropdown.Button>
      )}
      menu={onCollapse => (
        <Dropdown.Menu onCollapse={onCollapse} theme={DropdownMenuTheme.Onyx}>
          {LINE_COLOR_SCALES.map(({id, name, colors}) => (
            <Dropdown.Item
              key={id}
              id={id}
              value={colors}
              selected={findSelectedScaleID(value) === id}
              onClick={onChange}
            >
              <ColorSchemeDropdownItem
                name={name}
                colors={colors.map(c => c.hex)}
              />
            </Dropdown.Item>
          ))}
        </Dropdown.Menu>
      )}
    />
  )
}

export default ColorSchemeDropdown
