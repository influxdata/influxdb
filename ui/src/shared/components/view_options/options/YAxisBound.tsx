// Libraries
import React, {PureComponent} from 'react'

// Constants
import {AXES_SCALE_OPTIONS} from 'src/dashboards/constants/cellEditor'

// Components
import FormElement from 'src/clockface/components/form_layout/FormElement'
import OptIn from 'src/shared/components/OptIn'

interface Props {
  label: string
  bound: string
  scale: string
  onUpdateYAxisBound: (bound: string) => void
}

const {LOG} = AXES_SCALE_OPTIONS
const getInputMin = scale => (scale === LOG ? '0' : null)

class YAxisBound extends PureComponent<Props> {
  public render() {
    const {label, bound, scale, onUpdateYAxisBound} = this.props

    return (
      <FormElement label={label}>
        <OptIn
          customPlaceholder={'min'}
          customValue={bound}
          onSetValue={onUpdateYAxisBound}
          type="number"
          min={getInputMin(scale)}
        />
      </FormElement>
    )
  }
}

export default YAxisBound
