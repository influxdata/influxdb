// Libraries
import React, {PureComponent, ChangeEvent} from 'react'

// Components
import {
  Form,
  Input,
  Button,
  Grid,
  Columns,
  Dropdown,
} from '@influxdata/clockface'
import VariableArgumentsEditor from 'src/variables/components/VariableArgumentsEditor'

// Utils
import {validateVariableName} from 'src/variables/utils/validation'

// Constants
import {variableItemTypes} from 'src/variables/constants'

// Types
import {Props} from 'src/variables/components/VariableFormContext'
import {
  ButtonType,
  ComponentColor,
  ComponentStatus,
} from '@influxdata/clockface'
import {VariableArguments, VariableArgumentType} from 'src/types'

interface State {
  isNameValid: boolean
  hasValidArgs: boolean
  firstRun: boolean
  selected: string[]
}

export default class VariableForm extends PureComponent<Props, State> {
  constructor(props) {
    super(props)
    this.state = {
      isNameValid: false,
      hasValidArgs: false,
      firstRun: true,
      selected: null,
    }
  }

  public render() {
    const {name, variableType, onHideOverlay, submitButtonText} = this.props
    const {selected} = this.state

    const submitText = submitButtonText || 'Create Variable'

    return (
      <Form onSubmit={this.handleSubmit} testID="variable-form--root">
        <Grid>
          <Grid.Row>
            <Grid.Column widthXS={Columns.Six}>
              <div className="overlay-flux-editor--spacing">
                <Form.ValidationElement
                  label="Name"
                  value={name}
                  required={true}
                  validationFunc={this.handleNameValidation}
                >
                  {status => (
                    <Input
                      placeholder="Give your variable a name"
                      testID="variable-name-input"
                      name="name"
                      autoFocus={true}
                      value={name}
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
                  button={(active, onClick) => (
                    <Dropdown.Button
                      active={active}
                      onClick={onClick}
                      testID="variable-type-dropdown--button"
                    >
                      {this.typeDropdownLabel}
                    </Dropdown.Button>
                  )}
                  menu={onCollapse => (
                    <Dropdown.Menu onCollapse={onCollapse}>
                      {variableItemTypes.map(v => (
                        <Dropdown.Item
                          key={v.type}
                          id={v.type}
                          testID={`variable-type-dropdown-${v.type}`}
                          value={v.type}
                          onClick={this.handleChangeType}
                          selected={v.type === variableType}
                        >
                          {v.label}
                        </Dropdown.Item>
                      ))}
                    </Dropdown.Menu>
                  )}
                />
              </Form.Element>
            </Grid.Column>
          </Grid.Row>
          <Grid.Row>
            <Grid.Column>
              <VariableArgumentsEditor
                onChange={this.handleChangeArgs}
                onSelectMapDefault={this.handleSelectMapDefault}
                selected={selected}
                args={this.activeVariable}
              />
            </Grid.Column>
          </Grid.Row>
          <Grid.Row>
            <Grid.Column>
              <Form.Footer>
                <Button text="Cancel" onClick={onHideOverlay} />
                <Button
                  text={submitText}
                  type={ButtonType.Submit}
                  testID="variable-form-save"
                  color={ComponentColor.Success}
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
    )
  }

  private get typeDropdownLabel(): string {
    const {variableType} = this.props

    return variableItemTypes.find(variable => variable.type === variableType)
      .label
  }

  private get isFormValid(): boolean {
    const {isNameValid} = this.state

    return this.validArgs && isNameValid
  }

  private get activeVariable(): VariableArguments {
    const {initialScript, variableType, query, map, constant} = this.props
    const {firstRun} = this.state

    switch (variableType) {
      case 'query': {
        const _query = query
        if (firstRun && !_query.values.query.length) {
          _query.values.query = initialScript || ''
        }
        return _query
      }
      case 'map':
        return map
      case 'constant':
        return constant
    }

    return query
  }

  private get validArgs(): boolean {
    const {variableType, query, map, constant} = this.props

    switch (variableType) {
      case 'query':
        return !!query.values.query
      case 'constant':
        return constant.values.length !== 0
      case 'map':
        return Object.keys(map.values).length !== 0
      default:
        return false
    }
  }

  private handleSubmit = (): void => {
    const {name, onCreateVariable, onHideOverlay} = this.props
    const {selected} = this.state

    onCreateVariable({
      selected,
      name,
      arguments: this.activeVariable,
    } as any)

    onHideOverlay()
  }

  private handleChangeType = (selectedType: string) => {
    const {variableType, onTypeUpdate} = this.props
    const {isNameValid} = this.state
    const defaults = {selected: null, hasValidArgs: false, isNameValid}

    if (variableType === selectedType) {
      return
    }

    this.setState(defaults)

    onTypeUpdate(selectedType as VariableArgumentType)
  }

  private handleSelectMapDefault = (selected: string) => {
    this.setState({selected: [selected]})
  }

  private handleChangeArgs = ({
    args,
    isValid,
  }: {
    args: VariableArguments
    isValid: boolean
  }) => {
    const {onQueryUpdate, onMapUpdate, onConstantUpdate} = this.props

    switch (args.type) {
      case 'query':
        onQueryUpdate(args)
        break
      case 'map':
        onMapUpdate(args)
        break
      case 'constant':
        onConstantUpdate(args)
        break
    }

    this.setState({hasValidArgs: isValid})
  }

  private handleNameValidation = (name: string) => {
    const {variables} = this.props
    const {error} = validateVariableName(name, variables)

    this.setState({isNameValid: !error})

    return error
  }

  private handleChangeInput = (e: ChangeEvent<HTMLInputElement>) => {
    const {value} = e.target

    this.props.onNameUpdate(value)
  }
}
