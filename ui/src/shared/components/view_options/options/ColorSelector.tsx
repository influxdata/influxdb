// Libraries
import React, {PureComponent} from 'react'

// Components
import FormElement from 'src/clockface/components/form_layout/FormElement'
import ColorSchemeDropdown from 'src/shared/components/ColorSchemeDropdown'

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
    const {colors, onUpdateColors} = this.props

    return (
      <FormElement label="Line Colors">
        <ColorSchemeDropdown value={colors} onChange={onUpdateColors} />
      </FormElement>
    )
  }
}

export default LineGraphColorSelector
