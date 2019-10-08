// Libraries
import React, {PureComponent, ChangeEvent, FormEvent} from 'react'
import _ from 'lodash'
import {connect} from 'react-redux'

// Components
import {Form, Input, Button, Grid, Columns} from '@influxdata/clockface'

// Utils
import {validateVariableName} from 'src/variables/utils/validation'
import {extractVariablesList} from 'src/variables/selectors'

// Actions
import {updateVariable} from 'src/variables/actions'

// Types
import {AppState} from 'src/types'
import {IVariable as Variable} from '@influxdata/influx'
import {
  ButtonType,
  ComponentColor,
  ComponentStatus,
} from '@influxdata/clockface'

interface OwnProps {
  onDismiss: () => void
  variableID: string
}

interface State {
  workingVariable: Variable
  isNameValid: boolean
}

interface StateProps {
  variables: Variable[]
  startVariable: Variable
}

interface DispatchProps {
  onUpdateVariable: typeof updateVariable
}

type Props = StateProps & OwnProps & DispatchProps

class RenameVariableOverlayForm extends PureComponent<Props, State> {
  public state: State = {
    workingVariable: this.props.startVariable,
    isNameValid: true,
  }

  public render() {
    const {onDismiss} = this.props
    const {workingVariable, isNameValid} = this.state

    return (
      <Form onSubmit={this.handleSubmit}>
        <Grid>
          <Grid.Row>
            <Grid.Column widthXS={Columns.Twelve}>
              <div className="overlay-flux-editor--spacing">
                <Form.ValidationElement
                  label="Name"
                  value={workingVariable.name}
                  required={true}
                  validationFunc={this.handleNameValidation}
                >
                  {status => (
                    <Input
                      placeholder="Rename your variable"
                      name="name"
                      autoFocus={true}
                      value={workingVariable.name}
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
              <Form.Footer>
                <Button
                  text="Cancel"
                  color={ComponentColor.Danger}
                  onClick={onDismiss}
                />
                <Button
                  text="Submit"
                  type={ButtonType.Submit}
                  color={ComponentColor.Primary}
                  status={
                    isNameValid
                      ? ComponentStatus.Default
                      : ComponentStatus.Disabled
                  }
                />
              </Form.Footer>
            </Grid.Column>
          </Grid.Row>
        </Grid>
      </Form>
    )
  }

  private handleSubmit = (e: FormEvent): void => {
    const {workingVariable} = this.state

    e.preventDefault()

    this.props.onUpdateVariable(workingVariable.id, workingVariable)
    this.props.onDismiss()
  }

  private handleNameValidation = (name: string) => {
    const {variables} = this.props
    const {error} = validateVariableName(name, variables)

    this.setState({isNameValid: !error})

    return error
  }

  private handleChangeInput = (e: ChangeEvent<HTMLInputElement>) => {
    const name = e.target.value

    const workingVariable = {...this.state.workingVariable, name}

    this.setState({
      workingVariable,
    })
  }
}

const mstp = (state: AppState, {variableID}: OwnProps): StateProps => {
  const variables = extractVariablesList(state)
  const startVariable = variables.find(v => v.id === variableID)

  return {variables, startVariable}
}

const mdtp: DispatchProps = {
  onUpdateVariable: updateVariable,
}

export default connect<StateProps, DispatchProps, OwnProps>(
  mstp,
  mdtp
)(RenameVariableOverlayForm)
