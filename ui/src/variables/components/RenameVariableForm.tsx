// Libraries
import React, {PureComponent, ChangeEvent, FormEvent} from 'react'
import _ from 'lodash'
import {connect} from 'react-redux'
import {withRouter, RouteComponentProps} from 'react-router-dom'

// Components
import {Form, Input, Button, Grid, Columns} from '@influxdata/clockface'

// Utils
import {validateVariableName} from 'src/variables/utils/validation'
import {getVariables} from 'src/variables/selectors'

// Actions
import {updateVariable} from 'src/variables/actions/thunks'

// Types
import {AppState, Variable} from 'src/types'
import {
  ButtonType,
  ComponentColor,
  ComponentStatus,
} from '@influxdata/clockface'

interface OwnProps {
  onClose: () => void
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

type Props = StateProps &
  OwnProps &
  DispatchProps &
  RouteComponentProps<{orgID: string; id: string}>

class RenameVariableOverlayForm extends PureComponent<Props, State> {
  public state: State = {
    workingVariable: this.props.startVariable,
    isNameValid: true,
  }

  public render() {
    const {onClose} = this.props
    const {workingVariable, isNameValid} = this.state

    return (
      <Form onSubmit={this.handleSubmit}>
        <Grid>
          <Grid.Row>
            <Grid.Column widthXS={Columns.Six}>
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
                      testID="rename-variable-input"
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
                  onClick={onClose}
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
                  testID="rename-variable-submit"
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
    this.props.onClose()
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

const mstp = (state: AppState, {match}: Props): StateProps => {
  const variables = getVariables(state)
  const startVariable = variables.find(v => v.id === match.params.id)

  return {variables, startVariable}
}

const mdtp: DispatchProps = {
  onUpdateVariable: updateVariable,
}

export default withRouter(
  connect<StateProps, DispatchProps, OwnProps>(
    mstp,
    mdtp
  )(RenameVariableOverlayForm)
)
