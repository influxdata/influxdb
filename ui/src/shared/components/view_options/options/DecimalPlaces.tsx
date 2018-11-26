// Libraries
import React, {PureComponent} from 'react'

// Components
import {Form, Columns, AutoInput} from 'src/clockface'

// Constants
import {MIN_DECIMAL_PLACES, MAX_DECIMAL_PLACES} from 'src/dashboards/constants'

// Types
import {DecimalPlaces} from 'src/types/v2/dashboards'

// Decorators
import {ErrorHandling} from 'src/shared/decorators/errors'

interface Props extends DecimalPlaces {
  onDecimalPlacesChange: (decimalPlaces: DecimalPlaces) => void
}

@ErrorHandling
class DecimalPlacesOption extends PureComponent<Props> {
  constructor(props: Props) {
    super(props)
  }

  public render() {
    return (
      <Form.Element label="Decimal Places" colsXS={Columns.Six}>
        <AutoInput
          name="decimal-places"
          inputPlaceholder="Enter a number"
          onChange={this.handleSetValue}
          value={this.value}
          min={Number(MIN_DECIMAL_PLACES)}
          max={Number(MAX_DECIMAL_PLACES)}
        />
      </Form.Element>
    )
  }

  public handleSetValue = (value: number): void => {
    const digits = Math.max(value, 0)
    const isEnforced = true

    this.props.onDecimalPlacesChange({digits, isEnforced})
  }

  private get value(): number {
    const {isEnforced, digits} = this.props
    if (!isEnforced) {
      return
    }

    return digits
  }
}

export default DecimalPlacesOption
