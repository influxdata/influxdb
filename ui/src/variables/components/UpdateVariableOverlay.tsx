// Libraries
import React, {PureComponent, ChangeEvent, FormEvent} from 'react'
import _ from 'lodash'

// Components
import {
  Form,
  Input,
  Button,
  Grid,
  Dropdown,
  Columns,
} from '@influxdata/clockface'
import {Overlay} from 'src/clockface'
import VariableArgumentsEditor from 'src/variables/components/VariableArgumentsEditor'

// Utils
import {validateVariableName} from 'src/variables/utils/validation'

// Constants
import {variableItemTypes} from 'src/variables/constants'

// Types
import {IVariable as Variable} from '@influxdata/influx'
import {
  ButtonType,
  ComponentColor,
  ComponentStatus,
} from '@influxdata/clockface'
import {VariableArguments} from 'src/types'

interface Props {
  variable: Variable
  variables: Variable[]
  onCloseOverlay: () => void
  onUpdateVariable: (variable: Variable) => Promise<void>
}

interface State {
  workingVariable: Variable
  isNameValid: boolean
  hasValidArgs: boolean
}

export default class UpdateVariableOverlay extends PureComponent<Props, State> {
  public state: State = {
    workingVariable: this.props.variable,
    isNameValid: true,
    hasValidArgs: true,
  }

  public render() {
    const {onCloseOverlay} = this.props
    const {workingVariable} = this.state

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
                          placeholder="Give your variable a name"
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
                <Grid.Column widthXS={Columns.Six}>
                  <Form.Element label="Type" required={true}>
                    <Dropdown
                      selectedID={workingVariable.arguments.type}
                      onChange={this.handleChangeType}
                    >
                      {variableItemTypes.map(v => (
                        <Dropdown.Item key={v.type} id={v.type} value={v.type}>
                          {v.label}
                        </Dropdown.Item>
                      ))}
                    </Dropdown>
                  </Form.Element>
                </Grid.Column>
              </Grid.Row>
              <Grid.Row>
                <Grid.Column>
                  <VariableArgumentsEditor
                    onChange={this.handleChangeArgs}
                    onSelectMapDefault={this.handleSelectMapDefault}
                    selected={workingVariable.selected}
                    args={workingVariable.arguments}
                  />
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
                        this.isFormValid
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

  private get isFormValid(): boolean {
    const {hasValidArgs, isNameValid} = this.state

    return hasValidArgs && isNameValid
  }

  private handleChangeType = (selectedType: string) => {
    const {isNameValid, workingVariable} = this.state
    const defaults = {hasValidArgs: false, isNameValid}

    switch (selectedType) {
      case 'query':
        return this.setState({
          ...defaults,
          workingVariable: {
            ...workingVariable,
            arguments: {
              type: 'query',
              values: {
                query: '',
                language: 'flux',
              },
            },
            selected: null,
          },
        })
      case 'map':
        return this.setState({
          ...defaults,
          workingVariable: {
            ...workingVariable,
            selected: null,
            arguments: {
              type: 'map',
              values: {},
            },
          },
        })
      case 'constant':
        return this.setState({
          ...defaults,
          workingVariable: {
            ...workingVariable,
            selected: null,
            arguments: {
              type: 'constant',
              values: [],
            },
          },
        })
    }
  }

  private handleSelectMapDefault = (selected: string) => {
    const {workingVariable} = this.state

    this.setState({
      workingVariable: {
        ...workingVariable,
        selected: [selected],
      },
    })
  }

  private handleChangeArgs = ({
    args,
    isValid,
  }: {
    args: VariableArguments
    isValid: boolean
  }) => {
    const {workingVariable} = this.state

    this.setState({
      workingVariable: {
        ...workingVariable,
        arguments: args,
      },
      hasValidArgs: isValid,
    })
  }

  private handleSubmit = (e: FormEvent): void => {
    e.preventDefault()

    this.props.onUpdateVariable(this.state.workingVariable)
    this.props.onCloseOverlay()
  }

  private handleNameValidation = (name: string) => {
    const {variables} = this.props
    const {error} = validateVariableName(name, variables)

    this.setState({isNameValid: !error})

    return error
  }

  private handleChangeInput = (e: ChangeEvent<HTMLInputElement>) => {
    const value = e.target.value
    const key = e.target.name
    const workingVariable = {...this.state.workingVariable, [key]: value}

    this.setState({
      workingVariable,
    })
  }
}
