// Libraries
import React, {SFC, CSSProperties} from 'react'

// Components
import {Dropdown, DropdownMenuColors} from 'src/clockface'

// Constants
import {
  LINE_COLOR_SCALES,
  DEFAULT_LINE_COLORS,
} from 'src/shared/constants/graphColorPalettes'

// Styles
import 'src/shared/components/ColorSchemeDropdown.scss'

// Types
import {Color} from 'src/types/colors'

interface Props {
  value: Color[]
  onChange: (colors: Color[]) => void
}

const generateGradientStyle = (colors: Color[]): CSSProperties => {
  const [start, mid, stop] = colors.map(color => color.hex)

  return {
    background: `linear-gradient(to right, ${start} 0%, ${mid} 50%, ${stop} 100%)`,
  }
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
      customClass="color-scheme-dropdown"
    >
      {LINE_COLOR_SCALES.map(({id, name, colors}) => (
        <Dropdown.Item key={id} id={id} value={colors}>
          <div className="color-scheme-dropdown--item">
            <div
              className="color-scheme-dropdown--swatches"
              style={generateGradientStyle(colors)}
            />
            <div className="color-scheme-dropdown--name">{name}</div>
          </div>
        </Dropdown.Item>
      ))}
    </Dropdown>
  )
}

export default ColorSchemeDropdown
