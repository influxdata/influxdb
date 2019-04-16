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
import FluxEditor from 'src/shared/components/FluxEditor'

// Utils
import {validateVariableName} from 'src/variables/utils/validation'

// Constant
import {variableItemTypes} from 'src/variables/constants'

// Types
import {IVariable as Variable} from '@influxdata/influx'
import {
  ButtonType,
  ComponentColor,
  ComponentStatus,
} from '@influxdata/clockface'

interface Props {
  onCreateVariable: (variable: Pick<Variable, 'name' | 'arguments'>) => void
  onHideOverlay?: () => void
  initialScript?: string
  variables: Variable[]
}

interface State {
  name: string
  script: string
  isFormValid: boolean
  selectedType: string
}

export default class VariableForm extends PureComponent<Props, State> {
  constructor(props) {
    super(props)
    this.state = {
      name: '',
      script: this.props.initialScript || '',
      isFormValid: false,
      selectedType: 'query',
    }
  }

  public render() {
    const {onHideOverlay} = this.props
    const {name, script, isFormValid, selectedType} = this.state

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
                  selectedID={selectedType}
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
    )
  }

  private handleSubmit = (): void => {
    const {onCreateVariable, onHideOverlay} = this.props
    const {selectedType} = this.state

    if (selectedType === 'query') {
      onCreateVariable({
        name: this.state.name,
        arguments: {
          type: 'query',
          values: {query: this.state.script, language: 'flux'},
        },
      })
    }

    onHideOverlay()
  }

  private handleChangeType = (selectedType: string) => {
    this.setState({
      selectedType,
      script: '',
    })
  }

  private handleNameValidation = (name: string) => {
    const {variables} = this.props
    const {error} = validateVariableName(name, variables)

    this.setState({isFormValid: !error})

    return error
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
