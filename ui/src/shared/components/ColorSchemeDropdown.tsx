// Libraries
import React, {SFC} from 'react'

// Components
import {Dropdown, DropdownMenuTheme} from '@influxdata/clockface'
import ColorSchemeDropdownItem from 'src/shared/components/ColorSchemeDropdownItem'

// Constants
import {LINE_COLOR_SCALES} from 'src/shared/constants/graphColorPalettes'

// Types
import {Color} from 'src/types/colors'

interface Props {
  value: Color[]
  onChange: (colors: Color[]) => void
}

interface Scale {
  name: string
  id: string
  colors: Array<{hex: string}>
}

const findSelectedScale = (colors: Color[]): Scale => {
  const key = (colors: Color[]) => colors.map(color => color.hex).join(', ')
  const needle = key(colors)
  const selectedScale = LINE_COLOR_SCALES.find(
    d => key(d.colors as Color[]) === needle
  )

  if (selectedScale) {
    return selectedScale
  } else {
    return LINE_COLOR_SCALES[0]
  }
}

const ColorSchemeDropdown: SFC<Props> = ({value, onChange}) => {
  const selectedScale = findSelectedScale(value)

  return (
    <Dropdown
      button={(active, onClick) => (
        <Dropdown.Button active={active} onClick={onClick}>
          <ColorSchemeDropdownItem
            name={selectedScale.name}
            colors={selectedScale.colors.map(c => c.hex)}
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
              selected={selectedScale.id === id}
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
