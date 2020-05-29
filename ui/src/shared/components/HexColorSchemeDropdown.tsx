// Libraries
import React, {FunctionComponent} from 'react'

// Components
import {Dropdown, DropdownMenuTheme} from '@influxdata/clockface'
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
      className="color-scheme-dropdown"
      button={(active, onClick) => (
        <Dropdown.Button active={active} onClick={onClick}>
          <ColorSchemeDropdownItem
            name={selectedName}
            colors={selectedColorScheme}
          />
        </Dropdown.Button>
      )}
      menu={onCollapse => (
        <Dropdown.Menu onCollapse={onCollapse} theme={DropdownMenuTheme.Onyx}>
          {resolvedSchemes.map(({name, colors}) => (
            <Dropdown.Item
              key={name}
              id={name}
              value={colors}
              onClick={onSelectColorScheme}
              selected={selectedName === name}
            >
              <ColorSchemeDropdownItem name={name} colors={colors} />
            </Dropdown.Item>
          ))}
        </Dropdown.Menu>
      )}
    />
  )
}

export default HexColorSchemeDropdown
