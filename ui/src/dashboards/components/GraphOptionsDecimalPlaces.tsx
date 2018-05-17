import React, {PureComponent} from 'react'
import {ErrorHandling} from 'src/shared/decorators/errors'
import OptIn from 'src/shared/components/OptIn'

import {DecimalPlaces} from 'src/types/dashboard'

interface Props extends DecimalPlaces {
  onDecimalPlacesChange: (decimalPlaces: DecimalPlaces) => void
}

const fixedValueString = 'fixed'

@ErrorHandling
class GraphOptionsDecimalPlaces extends PureComponent<Props> {
  constructor(props: Props) {
    super(props)
  }

  public onSetValue = (valueFromSelector: string): void => {
    let digits
    let isEnforced
    if (valueFromSelector === fixedValueString) {
      digits = this.props.digits
      isEnforced = false
    } else if (valueFromSelector === '') {
      digits = this.props.digits
      isEnforced = true
    } else {
      digits = Number(valueFromSelector)
      if (digits < 0) {
        digits = 0
      }
      isEnforced = true
    }
    this.props.onDecimalPlacesChange({digits, isEnforced})
  }

  public render() {
    const {digits, isEnforced} = this.props
    return (
      <div className="form-group col-xs-6">
        <label> Decimal Places </label>
        <OptIn
          customPlaceholder={isEnforced ? digits.toString() : 'unlimited'}
          customValue={isEnforced ? digits.toString() : ''}
          onSetValue={this.onSetValue}
          fixedPlaceholder={''}
          fixedValue={fixedValueString}
          type="number"
          min={'0'}
        />
      </div>
    )
  }
}

export default GraphOptionsDecimalPlaces
