// Libraries
import React, {Component} from 'react'

// Components
import {Dropdown, DropdownMenuColors} from 'src/clockface'

// Constants
import {
  PRESET_LABEL_COLORS,
  CUSTOM_LABEL,
} from 'src/configuration/constants/LabelColors'

// Types
import {LabelColor, LabelColorType} from 'src/types/colors'

// Styles
import 'src/configuration/components/LabelColorDropdown.scss'

// Decorators
import {ErrorHandling} from 'src/shared/decorators/errors'

interface Props {
  colorHex: string
  onChange: (colorHex: string) => void
  onToggleCustomColorHex: (useCustomColorHex: boolean) => void
  useCustomColorHex: boolean
}

@ErrorHandling
class LabelColorDropdown extends Component<Props> {
  public render() {
    return (
      <Dropdown
        selectedID={this.selectedColorID}
        onChange={this.handleChange}
        menuColor={DropdownMenuColors.Onyx}
      >
        {PRESET_LABEL_COLORS.map(preset => {
          if (preset.type === LabelColorType.Preset) {
            return (
              <Dropdown.Item id={preset.id} key={preset.id} value={preset}>
                <div className="label-colors--item">
                  <div
                    className="label-colors--swatch"
                    style={{backgroundColor: preset.colorHex}}
                  />
                  {preset.name}
                </div>
              </Dropdown.Item>
            )
          } else if (preset.type === LabelColorType.Custom) {
            return (
              <Dropdown.Item id={preset.id} key={preset.id} value={preset}>
                <div className="label-colors--item">
                  <div className="label-colors--custom" />
                  {preset.name}
                </div>
              </Dropdown.Item>
            )
          }
        })}
      </Dropdown>
    )
  }

  private get selectedColorID(): string {
    const {colorHex, useCustomColorHex} = this.props

    const foundPreset = PRESET_LABEL_COLORS.find(
      preset => preset.colorHex === colorHex
    )

    if (useCustomColorHex || !foundPreset) {
      return CUSTOM_LABEL.id
    }

    return foundPreset.id
  }

  private handleChange = (color: LabelColor): void => {
    const {onChange, onToggleCustomColorHex} = this.props
    const {colorHex, type} = color

    if (type === LabelColorType.Preset) {
      onToggleCustomColorHex(false)
      onChange(colorHex)
    } else if (type === LabelColorType.Custom) {
      onToggleCustomColorHex(true)
    }
  }
}

export default LabelColorDropdown
