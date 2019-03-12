// Libraries
import React, {PureComponent, ChangeEvent, FormEvent} from 'react'
import _ from 'lodash'

// Components
import {Overlay, ComponentStatus, Form, Input} from 'src/clockface'
import {Button, ButtonType, ComponentColor} from '@influxdata/clockface'
import FluxEditor from 'src/shared/components/FluxEditor'

// Types
import {Variable} from '@influxdata/influx'

interface Props {
  variable: Variable
  onCloseOverlay: () => void
  onUpdateVariable: (variable: Variable) => Promise<void>
}

interface State {
  variable: Variable
  script: string
  nameErrorMessage: string
  nameInputStatus: ComponentStatus
}

export default class UpdateVariableOverlay extends PureComponent<Props, State> {
  constructor(props) {
    super(props)

    const {variable} = this.props
    const script = _.get(variable, 'arguments.values.query', '')

    this.state = {
      variable,
      script,
      nameInputStatus: ComponentStatus.Default,
      nameErrorMessage: '',
    }
  }

  public render() {
    const {onCloseOverlay} = this.props
    const {variable, nameInputStatus, nameErrorMessage, script} = this.state

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
                  script={script}
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
    const {variable, script} = this.state

    return !!variable.name && !!script
  }

  private handleSubmit = (e: FormEvent): void => {
    e.preventDefault()

    this.props.onUpdateVariable(this.state.variable)
    this.props.onCloseOverlay()
  }

  private handleChangeScript = (script: string): void => {
    this.setState({script})
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
