// Libraries
import React, {PureComponent, ChangeEvent, FormEvent} from 'react'
import _ from 'lodash'

// Components
import {
  Button,
  ComponentColor,
  ButtonType,
  Columns,
  ComponentStatus,
} from '@influxdata/clockface'
import {
  Grid,
  Form,
  Input,
  InputType,
  ComponentSpacer,
  Alignment,
} from 'src/clockface'
import RandomLabelColorButton from 'src/configuration/components/RandomLabelColor'
import {Label, LabelProperties} from 'src/types/v2/labels'

// Constants
import {HEX_CODE_CHAR_LENGTH} from 'src/configuration/constants/LabelColors'

// Utils
import {
  validateHexCode,
  randomPresetColor,
} from 'src/configuration/utils/labels'

// Decorators
import {ErrorHandling} from 'src/shared/decorators/errors'

// Style
import 'src/shared/components/ResourceLabelForm.scss'

interface Props {
  labelName: string
  onSubmit: (label: Label) => void
}

interface State {
  isValid: boolean
  label: Label
}

@ErrorHandling
export default class ResourceLabelForm extends PureComponent<Props, State> {
  public constructor(props: Props) {
    super(props)

    this.state = {
      isValid: true,
      label: {
        name: props.labelName,
        properties: {
          description: '',
          color: randomPresetColor(),
        },
      },
    }
  }

  public componentDidUpdate() {
    if (this.props.labelName !== this.state.label.name) {
      this.setState({label: {...this.state.label, name: this.props.labelName}})
    }
  }

  public render() {
    const {isValid} = this.state

    return (
      <div className="resource-label--form">
        <Grid.Row>
          <Grid.Column widthXS={Columns.Five}>
            <Form.Element label="Color">
              <ComponentSpacer stretchToFitWidth={true} align={Alignment.Left}>
                <RandomLabelColorButton
                  colorHex={this.colorHex}
                  onClick={this.handleColorChange}
                />
                {this.customColorInput}
              </ComponentSpacer>
            </Form.Element>
          </Grid.Column>
          <Grid.Column widthXS={Columns.Five}>
            <Form.Element label="Description">
              <Input
                type={InputType.Text}
                placeholder="Add a optional description"
                name="description"
                value={this.description}
                onChange={this.handleInputChange}
              />
            </Form.Element>
          </Grid.Column>
          <Grid.Column widthXS={Columns.Two}>
            <Form.Element label="">
              <Button
                customClass="resource-label--create-button"
                text="Create Label"
                color={ComponentColor.Success}
                type={ButtonType.Submit}
                status={
                  isValid ? ComponentStatus.Default : ComponentStatus.Disabled
                }
                onClick={this.handleSubmit}
              />
            </Form.Element>
          </Grid.Column>
        </Grid.Row>
      </div>
    )
  }

  private handleSubmit = (e: FormEvent) => {
    e.preventDefault()

    this.props.onSubmit(this.state.label)
  }

  private handleCustomColorChange = (
    e: ChangeEvent<HTMLInputElement>
  ): void => {
    const {value} = e.target

    if (validateHexCode(value)) {
      this.setState({isValid: false})
    } else {
      this.setState({isValid: true})
    }

    this.updateProperties({color: value})
  }

  private handleColorChange = (color: string) => {
    this.setState({isValid: true})

    this.updateProperties({color})
  }

  private handleInputChange = (e: ChangeEvent<HTMLInputElement>): void => {
    this.updateProperties({description: e.target.value})
  }

  private updateProperties(update: Partial<LabelProperties>) {
    const {label} = this.state

    this.setState({
      label: {
        ...label,
        properties: {...label.properties, ...update},
      },
    })
  }

  private get customColorInput(): JSX.Element {
    const {colorHex} = this

    return (
      <Form.ValidationElement
        label=""
        value={colorHex}
        validationFunc={validateHexCode}
      >
        {status => (
          <Input
            type={InputType.Text}
            value={colorHex}
            placeholder="#000000"
            onChange={this.handleCustomColorChange}
            status={status}
            maxLength={HEX_CODE_CHAR_LENGTH}
          />
        )}
      </Form.ValidationElement>
    )
  }

  private get colorHex(): string {
    return this.state.label.properties.color
  }

  private get description(): string {
    return this.state.label.properties.description
  }
}
