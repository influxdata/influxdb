// Libraries
import React, {PureComponent} from 'react'

// Components
import {Form, Grid} from '@influxdata/clockface'
import ColorSchemeDropdown from 'src/shared/components/ColorSchemeDropdown'

// Types
import {Columns} from '@influxdata/clockface'
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
      <Grid.Column widthXS={Columns.Twelve}>
        <Form.Element label="Line Colors">
          <ColorSchemeDropdown value={colors} onChange={onUpdateColors} />
        </Form.Element>
      </Grid.Column>
    )
  }
}

export default LineGraphColorSelector
