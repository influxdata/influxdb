// Libraries
import React, {PureComponent, ChangeEvent} from 'react'
import _ from 'lodash'

// Components
import {Input, FormElement, Grid} from '@influxdata/clockface'

// Utils
import {validateURI} from 'src/shared/utils/validateURI'

// Type
import {Columns, ComponentStatus, ComponentSize} from '@influxdata/clockface'

const VALIDATE_DEBOUNCE_MS = 350

interface Props {
  name: string
  autoFocus?: boolean
  value: string
  helpText: string
  onChange: (e: ChangeEvent<HTMLInputElement>) => void
}

interface State {
  status: ComponentStatus
}

class URIFormElement extends PureComponent<Props, State> {
  private debouncedValidate: (value: string) => void

  constructor(props) {
    super(props)
    this.state = {
      status: ComponentStatus.Default,
    }

    this.debouncedValidate = _.debounce(
      this.handleValidateURI,
      VALIDATE_DEBOUNCE_MS
    )
  }

  public render() {
    const {name, value, autoFocus, helpText} = this.props

    return (
      <Grid>
        <Grid.Row>
          <Grid.Column widthXS={Columns.Eight} offsetXS={Columns.Two}>
            <FormElement
              label={name}
              key={name}
              errorMessage={this.errorMessage}
              helpText={helpText}
            >
              <Input
                name={name}
                autoFocus={autoFocus}
                status={this.state.status}
                onChange={this.handleChange}
                size={ComponentSize.Medium}
                value={value}
              />
            </FormElement>
          </Grid.Column>
        </Grid.Row>
      </Grid>
    )
  }

  private get errorMessage(): string | null {
    const {status} = this.state

    if (status === ComponentStatus.Error) {
      return 'Must be a valid URI.'
    }
  }

  private handleChange = (e: ChangeEvent<HTMLInputElement>): void => {
    const {onChange} = this.props
    const {value} = e.target

    onChange(e)
    this.debouncedValidate(value)
  }

  private handleValidateURI = (value: string): void => {
    if (validateURI(value)) {
      this.setState({status: ComponentStatus.Valid})
    } else {
      this.setState({status: ComponentStatus.Error})
    }
  }
}

export default URIFormElement
