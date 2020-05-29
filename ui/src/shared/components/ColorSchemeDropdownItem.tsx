// Libraries
import React, {CSSProperties, FunctionComponent} from 'react'

const generateGradientStyle = (colors: string[]): CSSProperties => {
  const stops = colors
    .map((hex, i) => `${hex} ${Math.round((i / (colors.length - 1)) * 100)}%`)
    .join(', ')

  return {
    background: `linear-gradient(to right, ${stops})`,
  }
}

interface Props {
  name: string
  colors: string[]
}

const ColorSchemeDropdownItem: FunctionComponent<Props> = ({name, colors}) => {
  return (
    <div className="color-scheme-dropdown-item">
      <div
        className="color-scheme-dropdown-item--swatches"
        style={generateGradientStyle(colors)}
      />
      <div className="color-scheme-dropdown-item--name">{name}</div>
    </div>
  )
}

export default ColorSchemeDropdownItem
