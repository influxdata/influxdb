// Libraries
import React, {PureComponent, ChangeEvent} from 'react'

// Components
import {Form, Input, Button, Grid} from '@influxdata/clockface'
import FluxEditor from 'src/shared/components/FluxEditor'

// Types
import {Variable} from '@influxdata/influx'
import {
  ButtonType,
  ComponentColor,
  ComponentStatus,
} from '@influxdata/clockface'

interface Props {
  onCreateVariable: (variable: Pick<Variable, 'name' | 'arguments'>) => void
  onHideOverlay?: () => void
  initialScript?: string
}

interface State {
  name: string
  script: string
  nameInputStatus: ComponentStatus
  errorMessage: string
}

export default class VariableForm extends PureComponent<Props, State> {
  constructor(props) {
    super(props)
    this.state = {
      name: '',
      script: this.props.initialScript || '',
      nameInputStatus: ComponentStatus.Default,
      errorMessage: '',
    }
  }

  public render() {
    const {onHideOverlay} = this.props
    const {nameInputStatus, name, script} = this.state

    return (
      <Form onSubmit={this.handleSubmit}>
        <Grid>
          <Grid.Row>
            <Grid.Column>
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
            </Grid.Column>
            <Grid.Column>
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
            </Grid.Column>
            <Grid.Column>
              <Form.Footer>
                <Button
                  text="Cancel"
                  color={ComponentColor.Danger}
                  onClick={onHideOverlay}
                />
                <Button
                  text="Create"
                  type={ButtonType.Submit}
                  color={ComponentColor.Primary}
                />
              </Form.Footer>
            </Grid.Column>
          </Grid.Row>
        </Grid>
      </Form>
    )
  }

  private handleSubmit = (): void => {
    const {onCreateVariable, onHideOverlay} = this.props

    onCreateVariable({
      name: this.state.name,
      arguments: {
        type: 'query',
        values: {query: this.state.script, language: 'flux'},
      },
    })

    onHideOverlay()
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
