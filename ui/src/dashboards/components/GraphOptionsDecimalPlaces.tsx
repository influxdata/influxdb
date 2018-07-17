import React, {PureComponent} from 'react'
import {ErrorHandling} from 'src/shared/decorators/errors'
import OptIn from 'src/shared/components/OptIn'
import {MIN_DECIMAL_PLACES, MAX_DECIMAL_PLACES} from 'src/dashboards/constants'

import {DecimalPlaces} from 'src/types/dashboards'

interface Props extends DecimalPlaces {
  onDecimalPlacesChange: (decimalPlaces: DecimalPlaces) => void
}

const fixedValueString = 'fixed'
const defaultPlaceholder = 'unlimited'

@ErrorHandling
class GraphOptionsDecimalPlaces extends PureComponent<Props> {
  constructor(props: Props) {
    super(props)
  }

  public handleSetValue = (valueFromSelector: string): void => {
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
    return (
      <div className="form-group col-xs-6">
        <label> Decimal Places </label>
        <OptIn
          min={MIN_DECIMAL_PLACES}
          max={MAX_DECIMAL_PLACES}
          type="number"
          fixedPlaceholder=""
          customValue={this.value}
          fixedValue={fixedValueString}
          onSetValue={this.handleSetValue}
          customPlaceholder={this.placeholder}
        />
      </div>
    )
  }

  private get placeholder(): string {
    const {isEnforced, digits} = this.props

    if (!isEnforced) {
      return defaultPlaceholder
    }

    return `${digits}`
  }

  private get value(): string {
    const {isEnforced, digits} = this.props
    if (!isEnforced) {
      return ''
    }

    return `${digits}`
  }
}

export default GraphOptionsDecimalPlaces
