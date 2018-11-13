// Libraries
import React, {PureComponent} from 'react'

// Components
import FormElement from 'src/clockface/components/form_layout/FormElement'
import OptIn from 'src/shared/components/OptIn'

interface Props {
  label: string
  onUpdateYAxisLabel: (label: string) => void
}
class YAxisTitle extends PureComponent<Props> {
  public render() {
    const {label, onUpdateYAxisLabel} = this.props

    return (
      <FormElement label="Title">
        <OptIn
          type="text"
          customValue={label}
          onSetValue={onUpdateYAxisLabel}
          customPlaceholder={this.defaultYLabel || 'y-axis title'}
        />
      </FormElement>
    )
  }

  private get defaultYLabel() {
    return ''
  }
}

export default YAxisTitle
