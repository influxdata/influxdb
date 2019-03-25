// Libraries
import React, {PureComponent, ChangeEvent, FormEvent} from 'react'
import _ from 'lodash'

// Components
import {Form, Input, Button} from '@influxdata/clockface'
import {Overlay} from 'src/clockface'
import FluxEditor from 'src/shared/components/FluxEditor'

// Types
import {Variable} from '@influxdata/influx'
import {
  ButtonType,
  ComponentColor,
  ComponentStatus,
} from '@influxdata/clockface'

interface Props {
  variable: Variable
  onCloseOverlay: () => void
  onUpdateVariable: (variable: Variable) => Promise<void>
}

interface State {
  variable: Variable
  nameErrorMessage: string
  nameInputStatus: ComponentStatus
}

export default class UpdateVariableOverlay extends PureComponent<Props, State> {
  public state: State = {
    variable: this.props.variable,
    nameInputStatus: ComponentStatus.Default,
    nameErrorMessage: '',
  }

  public render() {
    const {onCloseOverlay} = this.props
    const {variable, nameInputStatus, nameErrorMessage} = this.state

    return (
      <Overlay.Container maxWidth={1000}>
        <Overlay.Heading
          title="Edit Variable"
          onDismiss={this.props.onCloseOverlay}
        />

        <Form onSubmit={this.handleSubmit}>
          <Overlay.Body>
            <div className="overlay-flux-editor--spacing">
              <Form.Element label="Name" errorMessage={nameErrorMessage}>
                <Input
                  placeholder="Give your variable a name"
                  name="name"
                  autoFocus={true}
                  value={variable.name}
                  onChange={this.handleChangeInput}
                  status={nameInputStatus}
                />
              </Form.Element>
            </div>

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

            <Overlay.Footer>
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
                  this.isVariableValid
                    ? ComponentStatus.Default
                    : ComponentStatus.Disabled
                }
              />
            </Overlay.Footer>
          </Overlay.Body>
        </Form>
      </Overlay.Container>
    )
  }

  private get isVariableValid(): boolean {
    return !!this.state.variable.name && !!this.script
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

  private handleChangeInput = (e: ChangeEvent<HTMLInputElement>) => {
    const value = e.target.value
    const key = e.target.name
    const variable = {...this.state.variable, [key]: value}

    if (!value) {
      return this.setState({
        variable,
        nameInputStatus: ComponentStatus.Error,
        nameErrorMessage: `Variable ${key} cannot be empty`,
      })
    }

    this.setState({
      variable,
      nameInputStatus: ComponentStatus.Valid,
      nameErrorMessage: '',
    })
  }
}
