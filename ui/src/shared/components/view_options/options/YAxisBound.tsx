// Libraries
import React, {PureComponent} from 'react'

// Constants
import {AXES_SCALE_OPTIONS} from 'src/dashboards/constants/cellEditor'
const {LOG} = AXES_SCALE_OPTIONS

// Components
import {Form, Columns, AutoInput} from 'src/clockface'

interface Props {
  label: string
  bound: string
  scale: string
  onUpdateYAxisBound: (bound: string) => void
}

class YAxisBound extends PureComponent<Props> {
  public render() {
    const {label, bound} = this.props

    return (
      <Form.Element label={label} colsXS={Columns.Six}>
        <AutoInput
          name={bound}
          inputPlaceholder="Enter a number"
          value={Number(bound)}
          onChange={this.handleChange}
          min={this.inputMin}
        />
      </Form.Element>
    )
  }

  private get inputMin(): number {
    const {scale} = this.props

    if (scale === LOG) {
      return 0
    }
  }

  private handleChange = (value: number) => {
    const {onUpdateYAxisBound} = this.props
    const bound = `${value}`

    onUpdateYAxisBound(bound)
  }
}

export default YAxisBound
