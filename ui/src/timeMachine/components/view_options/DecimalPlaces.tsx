// Libraries
import React, {PureComponent, ChangeEvent} from 'react'

// Utils
import {convertUserInputToNumOrNaN} from 'src/shared/utils/convertUserInput'

// Components
import {
  Form,
  Grid,
  AutoInput,
  AutoInputMode,
  Input,
  InputType,
} from '@influxdata/clockface'

// Constants
import {MIN_DECIMAL_PLACES, MAX_DECIMAL_PLACES} from 'src/dashboards/constants'

// Types
import {DecimalPlaces} from 'src/types'

// Decorators
import {ErrorHandling} from 'src/shared/decorators/errors'

interface Props extends DecimalPlaces {
  onDecimalPlacesChange: (decimalPlaces: DecimalPlaces) => void
}

interface State {
  mode: AutoInputMode
  value: number
}

@ErrorHandling
class DecimalPlacesOption extends PureComponent<Props, State> {
  constructor(props: Props) {
    super(props)

    this.state = {
      mode: this.props.digits ? AutoInputMode.Custom : AutoInputMode.Auto,
      value: this.props.digits,
    }
  }

  public render() {
    const {mode} = this.state

    return (
      <Grid.Column>
        <Form.Element label="Decimal Places">
          <AutoInput
            mode={mode}
            onChangeMode={this.handleChangeMode}
            inputComponent={
              <Input
                name="decimal-places"
                placeholder="Enter a number"
                onFocus={this.handleSetValue}
                onChange={this.handleSetValue}
                value={this.state.value}
                min={MIN_DECIMAL_PLACES}
                max={MAX_DECIMAL_PLACES}
                type={InputType.Number}
              />
            }
          />
        </Form.Element>
      </Grid.Column>
    )
  }

  public handleSetValue = (event: ChangeEvent<HTMLInputElement>): void => {
    const value = convertUserInputToNumOrNaN(event)
    const {digits, onDecimalPlacesChange} = this.props

    if (value === value && value >= MIN_DECIMAL_PLACES) {
      onDecimalPlacesChange({
        digits: Math.min(value, MAX_DECIMAL_PLACES),
        isEnforced: true,
      })
    } else {
      onDecimalPlacesChange({digits, isEnforced: false})
    }
    this.setState({value})
  }

  private handleChangeMode = (mode: AutoInputMode): void => {
    this.setState({mode})
  }
}

export default DecimalPlacesOption
