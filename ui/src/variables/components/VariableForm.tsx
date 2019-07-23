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
import {IVariable as Variable} from '@influxdata/influx'
import {
  ButtonType,
  ComponentColor,
  ComponentStatus,
} from '@influxdata/clockface'
import {VariableArguments} from 'src/types'

interface Props {
  onCreateVariable: (
    variable: Pick<Variable, 'name' | 'arguments' | 'selected'>
  ) => void
  onHideOverlay?: () => void
  initialScript?: string
  variables: Variable[]
}

interface State {
  name: string
  args: VariableArguments
  isNameValid: boolean
  hasValidArgs: boolean
  selected: string[]
}

export default class VariableForm extends PureComponent<Props, State> {
  constructor(props) {
    super(props)
    this.state = {
      name: '',
      args: {
        type: 'query',
        values: {
          query: this.props.initialScript || '',
          language: 'flux',
        },
      },
      isNameValid: false,
      hasValidArgs: false,
      selected: null,
    }
  }

  public render() {
    const {onHideOverlay} = this.props
    const {name, args, selected} = this.state

    return (
      <Form onSubmit={this.handleSubmit}>
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
                    <Dropdown.Button active={active} onClick={onClick}>
                      {this.typeDropdownLabel}
                    </Dropdown.Button>
                  )}
                  menu={onCollapse => (
                    <Dropdown.Menu onCollapse={onCollapse}>
                      {variableItemTypes.map(v => (
                        <Dropdown.Item
                          key={v.type}
                          id={v.type}
                          value={v.type}
                          onClick={this.handleChangeType}
                          selected={v.type === args.type}
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
                args={args}
              />
            </Grid.Column>
          </Grid.Row>
          <Grid.Row>
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
    const {args} = this.state

    return variableItemTypes.find(variable => variable.type === args.type).label
  }

  private get isFormValid(): boolean {
    const {hasValidArgs, isNameValid} = this.state

    return hasValidArgs && isNameValid
  }

  private handleSubmit = (): void => {
    const {onCreateVariable, onHideOverlay} = this.props
    const {args, name, selected} = this.state

    onCreateVariable({
      selected,
      name,
      arguments: args,
    })

    onHideOverlay()
  }

  private handleChangeType = (selectedType: string) => {
    const {isNameValid} = this.state
    const defaults = {selected: null, hasValidArgs: false, isNameValid}

    switch (selectedType) {
      case 'query':
        return this.setState({
          ...defaults,
          args: {
            type: 'query',
            values: {
              query: '',
              language: 'flux',
            },
          },
        })
      case 'map':
        return this.setState({
          ...defaults,
          args: {
            type: 'map',
            values: {},
          },
        })
      case 'constant':
        return this.setState({
          ...defaults,
          args: {
            type: 'constant',
            values: [],
          },
        })
    }
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
    this.setState({args, hasValidArgs: isValid})
  }

  private handleNameValidation = (name: string) => {
    const {variables} = this.props
    const {error} = validateVariableName(name, variables)

    this.setState({isNameValid: !error})

    return error
  }

  private handleChangeInput = (e: ChangeEvent<HTMLInputElement>) => {
    const {value, name} = e.target

    const newState = {...this.state}
    newState[name] = value
    this.setState(newState)
  }
}
