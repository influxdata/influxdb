// Libraries
import React, {FunctionComponent} from 'react'

// Components
import {Dropdown, DropdownMenuColors} from 'src/clockface'
import ColorSchemeDropdownItem from 'src/shared/components/ColorSchemeDropdownItem'

interface Props {
  colorSchemes: Array<{name: string; colors: string[]}>
  selectedColorScheme: string[]
  onSelectColorScheme: (scheme: string[]) => void
}

const HexColorSchemeDropdown: FunctionComponent<Props> = ({
  colorSchemes,
  selectedColorScheme,
  onSelectColorScheme,
}) => {
  const selected = colorSchemes.find(
    scheme =>
      scheme.colors.length === selectedColorScheme.length &&
      scheme.colors.every((color, i) => color === selectedColorScheme[i])
  )

  let selectedName
  let resolvedSchemes

  if (selected) {
    selectedName = selected.name
    resolvedSchemes = colorSchemes
  } else {
    selectedName = 'Custom'
    resolvedSchemes = [
      ...colorSchemes,
      {name: 'Custom', colors: selectedColorScheme},
    ]
  }

  return (
    <Dropdown
      selectedID={selectedName}
      onChange={onSelectColorScheme}
      menuColor={DropdownMenuColors.Onyx}
      customClass="color-scheme-dropdown"
    >
      {resolvedSchemes.map(({name, colors}) => (
        <Dropdown.Item key={name} id={name} value={colors}>
          <ColorSchemeDropdownItem name={name} colors={colors} />
        </Dropdown.Item>
      ))}
    </Dropdown>
  )
}

export default HexColorSchemeDropdown
