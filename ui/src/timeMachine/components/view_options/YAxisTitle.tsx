// Libraries
import React, {PureComponent, ChangeEvent} from 'react'

// Components
import {Form, Input, Grid} from '@influxdata/clockface'

// Types
import {Columns} from '@influxdata/clockface'

interface Props {
  label: string
  onUpdateYAxisLabel: (label: string) => void
}
class YAxisTitle extends PureComponent<Props> {
  public render() {
    const {label} = this.props

    return (
      <Grid.Column widthXS={Columns.Twelve}>
        <Form.Element label="Y Axis Label">
          <Input value={label} onChange={this.handleChange} />
        </Form.Element>
      </Grid.Column>
    )
  }

  private handleChange = (e: ChangeEvent<HTMLInputElement>) => {
    const {onUpdateYAxisLabel} = this.props

    onUpdateYAxisLabel(e.target.value)
  }
}

export default YAxisTitle
