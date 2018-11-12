// Libraries
import React, {PureComponent} from 'react'

// Components
import FormElement from 'src/clockface/components/form_layout/FormElement'
import ColorScaleDropdown from 'src/shared/components/ColorScaleDropdown'

// Types
import {Color} from 'src/types/colors'
import {ErrorHandling} from 'src/shared/decorators/errors'

interface Props {
  colors: Color[]
  onUpdateColors: (colors: Color[]) => void
}

@ErrorHandling
class LineGraphColorSelector extends PureComponent<Props> {
  public render() {
    const {colors} = this.props

    return (
      <FormElement label="Line Colors">
        <ColorScaleDropdown
          onChoose={this.handleSelectColors}
          stretchToFit={true}
          selected={colors}
        />
      </FormElement>
    )
  }

  public handleSelectColors = (colorScale): void => {
    const {onUpdateColors} = this.props
    const {colors} = colorScale

    onUpdateColors(colors)
  }
}

export default LineGraphColorSelector
