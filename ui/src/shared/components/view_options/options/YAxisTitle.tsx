// Libraries
import React, {PureComponent, ChangeEvent} from 'react'

// Components
import {Input, FormElement} from 'src/clockface'

interface Props {
  label: string
  onUpdateYAxisLabel: (label: string) => void
}
class YAxisTitle extends PureComponent<Props> {
  public render() {
    const {label} = this.props

    return (
      <FormElement label="Title">
        <Input value={label} onChange={this.handleChange} />
      </FormElement>
    )
  }

  private handleChange = (e: ChangeEvent<HTMLInputElement>) => {
    const {onUpdateYAxisLabel} = this.props

    onUpdateYAxisLabel(e.target.value)
  }
}

export default YAxisTitle
