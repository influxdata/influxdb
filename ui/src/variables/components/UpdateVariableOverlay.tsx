// Libraries
import React, {PureComponent, ChangeEvent, FormEvent} from 'react'
import _ from 'lodash'

// Components
import {Form, Input, Button, Grid} from '@influxdata/clockface'
import {Overlay} from 'src/clockface'
import FluxEditor from 'src/shared/components/FluxEditor'

// Utils
import {validateVariableName} from 'src/variables/utils/validation'

// Types
import {IVariable as Variable} from '@influxdata/influx'
import {
  ButtonType,
  ComponentColor,
  ComponentStatus,
} from '@influxdata/clockface'

interface Props {
  variable: Variable
  variables: Variable[]
  onCloseOverlay: () => void
  onUpdateVariable: (variable: Variable) => Promise<void>
}

interface State {
  variable: Variable
  isFormValid: boolean
}

export default class UpdateVariableOverlay extends PureComponent<Props, State> {
  public state: State = {
    variable: this.props.variable,
    isFormValid: true,
  }

  public render() {
    const {onCloseOverlay} = this.props
    const {variable, isFormValid} = this.state

    return (
      <Overlay.Container maxWidth={1000}>
        <Overlay.Heading
          title="Edit Variable"
          onDismiss={this.props.onCloseOverlay}
        />
        <Overlay.Body>
          <Form onSubmit={this.handleSubmit}>
            <Grid>
              <Grid.Row>
                <Grid.Column>
                  <div className="overlay-flux-editor--spacing">
                    <Form.ValidationElement
                      label="Name"
                      value={variable.name}
                      required={true}
                      validationFunc={this.handleNameValidation}
                    >
                      {status => (
                        <Input
                          placeholder="Give your variable a name"
                          name="name"
                          autoFocus={true}
                          value={variable.name}
                          onChange={this.handleChangeInput}
                          status={status}
                        />
                      )}
                    </Form.ValidationElement>
                  </div>
                </Grid.Column>
              </Grid.Row>
              <Grid.Row>
                <Grid.Column>
                  <Form.Element label="Value">
                    <div className="overlay-flux-editor">
                      <FluxEditor
                        script={this.script}
                        onChangeScript={this.handleChangeScript}
                        visibility="visible"
                        suggestions={[]}
                      />
                    </div>
                  </Form.Element>
                </Grid.Column>
              </Grid.Row>
              <Grid.Row>
                <Grid.Column>
                  <Form.Footer>
                    <Button
                      text="Cancel"
                      color={ComponentColor.Danger}
                      onClick={onCloseOverlay}
                    />
                    <Button
                      text="Submit"
                      type={ButtonType.Submit}
                      color={ComponentColor.Primary}
                      status={
                        isFormValid
                          ? ComponentStatus.Default
                          : ComponentStatus.Disabled
                      }
                    />
                  </Form.Footer>
                </Grid.Column>
              </Grid.Row>
            </Grid>
          </Form>
        </Overlay.Body>
      </Overlay.Container>
    )
  }

  private get script(): string {
    return _.get(this.state, 'variable.arguments.values.query', '')
  }

  private handleSubmit = (e: FormEvent): void => {
    e.preventDefault()

    this.props.onUpdateVariable(this.state.variable)
    this.props.onCloseOverlay()
  }

  private handleChangeScript = (script: string): void => {
    const {variable} = this.state

    if (variable.arguments.type !== 'query') {
      throw new Error('updating non-query variable not implemented')
    }

    const newVariable = {
      ...variable,
      arguments: {
        type: 'query',
        values: {
          query: script,
          language: 'flux',
        },
      },
    }

    this.setState({variable: newVariable})
  }

  private handleNameValidation = (name: string) => {
    const {variables} = this.props
    const {error} = validateVariableName(name, variables)

    this.setState({isFormValid: !error})

    return error
  }

  private handleChangeInput = (e: ChangeEvent<HTMLInputElement>) => {
    const value = e.target.value
    const key = e.target.name
    const variable = {...this.state.variable, [key]: value}

    this.setState({
      variable,
    })
  }
}
