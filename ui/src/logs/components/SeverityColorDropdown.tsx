// Libraries
import React, {Component} from 'react'

// Components
import ColorDropdown from 'src/shared/components/ColorDropdown'

// Utils
import {ErrorHandling} from 'src/shared/decorators/errors'

// Constants
import {SEVERITY_COLORS} from 'src/logs/constants'

// Types
import {ColorLabel} from 'src/types/colors'

interface Props {
  selected: ColorLabel
  disabled?: boolean
  stretchToFit?: boolean
  onChoose: (severityLevel: string, color: ColorLabel) => void
  severityLevel: string
}

interface State {
  expanded: boolean
}

@ErrorHandling
export default class SeverityColorDropdown extends Component<Props, State> {
  public render() {
    const {disabled, stretchToFit, selected} = this.props
    return (
      <ColorDropdown
        colors={SEVERITY_COLORS}
        disabled={disabled}
        stretchToFit={stretchToFit}
        selected={selected}
        onChoose={this.handleColorClick}
      />
    )
  }

  private handleColorClick = (color: ColorLabel): void => {
    this.props.onChoose(this.props.severityLevel, color)
  }
}
