// Libraries
import React, {PureComponent, ChangeEvent} from 'react'

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
}

@ErrorHandling
class DecimalPlacesOption extends PureComponent<Props, State> {
  constructor(props: Props) {
    super(props)

    this.state = {
      mode: this.props.digits ? AutoInputMode.Custom : AutoInputMode.Auto,
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
                onChange={this.handleSetValue}
                value={this.value}
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

  public handleSetValue = (e: ChangeEvent<HTMLInputElement>): void => {
    const value = Number(e.target.value) || 0
    const {digits, onDecimalPlacesChange} = this.props

    if (value === null) {
      onDecimalPlacesChange({digits, isEnforced: false})
    } else {
      onDecimalPlacesChange({digits: value, isEnforced: true})
    }
  }

  private handleChangeMode = (mode: AutoInputMode): void => {
    this.setState({mode})
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
