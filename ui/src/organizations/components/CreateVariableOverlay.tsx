// Libraries
import React, {PureComponent, ChangeEvent} from 'react'

// Styles
import 'src/organizations/components/CreateVariableOverlay.scss'

// Components
import {Form, Input, Overlay} from 'src/clockface'
import {Button} from '@influxdata/clockface'
import FluxEditor from 'src/shared/components/FluxEditor'

// Types
import {Variable} from '@influxdata/influx'
import {
  ComponentColor,
  ComponentStatus,
  ButtonType,
} from '@influxdata/clockface'

interface Props {
  onCreateVariable: (variable: Variable) => void
  onCloseModal: () => void
  orgID: string
}

interface State {
  name: string
  script: string
  nameInputStatus: ComponentStatus
  errorMessage: string
}

export default class CreateOrgOverlay extends PureComponent<Props, State> {
  constructor(props) {
    super(props)
    this.state = {
      name: '',
      script: '',
      nameInputStatus: ComponentStatus.Default,
      errorMessage: '',
    }
  }

  public render() {
    const {onCloseModal} = this.props
    const {nameInputStatus, name, script} = this.state

    return (
      <Overlay.Container maxWidth={1000}>
        <Overlay.Heading
          title="Create Variable"
          onDismiss={this.props.onCloseModal}
        />

        <Form onSubmit={this.handleSubmit}>
          <Overlay.Body>
            <div className="overlay-flux-editor--spacing">
              <Form.Element label="Name">
                <Input
                  placeholder="Give your variable a name"
                  name="name"
                  autoFocus={true}
                  value={name}
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
                onClick={onCloseModal}
              />
              <Button
                text="Create"
                type={ButtonType.Submit}
                color={ComponentColor.Primary}
              />
            </Overlay.Footer>
          </Overlay.Body>
        </Form>
      </Overlay.Container>
    )
  }

  private handleSubmit = (): void => {
    const {onCreateVariable, orgID, onCloseModal} = this.props

    onCreateVariable({
      name: this.state.name,
      orgID,
      arguments: {
        type: 'query',
        values: {query: this.state.script, language: 'flux'},
      },
    })

    onCloseModal()
  }

  private handleChangeInput = (e: ChangeEvent<HTMLInputElement>) => {
    const {value, name} = e.target

    const newState = {...this.state}
    newState[name] = value
    this.setState(newState)
  }

  private handleChangeScript = (script: string): void => {
    this.setState({script})
  }
}
