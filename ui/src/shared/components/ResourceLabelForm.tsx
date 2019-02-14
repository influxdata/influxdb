// Libraries
import React, {PureComponent, ChangeEvent, FormEvent} from 'react'
import _ from 'lodash'

// Components
import {
  Button,
  ComponentColor,
  ButtonType,
  Columns,
  Alignment,
  Stack,
  ComponentStatus,
} from '@influxdata/clockface'
import {Grid, Form, Input, ComponentSpacer, InputType} from 'src/clockface'
import LabelColorDropdown from 'src/configuration/components/LabelColorDropdown'
import {Label, LabelProperties} from 'src/types/v2/labels'

// Constants
import {
  CUSTOM_LABEL,
  HEX_CODE_CHAR_LENGTH,
  PRESET_LABEL_COLORS,
} from 'src/configuration/constants/LabelColors'

// Utils
import {validateHexCode} from 'src/configuration/utils/labels'

// Styles
import 'src/configuration/components/LabelOverlayForm.scss'

// Decorators
import {ErrorHandling} from 'src/shared/decorators/errors'

interface Props {
  labelName: string
  onSubmit: (label: Label) => void
}

interface State {
  isCustomColor: boolean
  label: Label
}

@ErrorHandling
export default class ResourceLabelForm extends PureComponent<Props, State> {
  public constructor(props: Props) {
    super(props)

    this.state = {
      isCustomColor: false,
      label: {
        name: props.labelName,
        properties: {
          description: '',
          color: _.sample(PRESET_LABEL_COLORS.slice(1)).colorHex,
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
    const {isCustomColor} = this.state

    return (
      <Grid>
        <Grid.Row>
          <Grid.Column widthSM={Columns.Ten}>
            <Grid.Column widthXS={Columns.Six}>
              <ComponentSpacer
                align={Alignment.Left}
                stackChildren={Stack.Rows}
              >
                <LabelColorDropdown
                  colorHex={this.dropdownColorHex}
                  onChange={this.handleColorChange}
                  useCustomColorHex={isCustomColor}
                  onToggleCustomColorHex={this.handleToggleCustomColorHex}
                />
                {this.customColorInput}
              </ComponentSpacer>
            </Grid.Column>
            <Grid.Column widthXS={Columns.Six}>
              <Input
                type={InputType.Text}
                placeholder="Add a optional description"
                name="description"
                value={this.description}
                onChange={this.handleInputChange}
              />
            </Grid.Column>
          </Grid.Column>
          <Grid.Column widthXS={Columns.Two}>
            <ComponentSpacer align={Alignment.Center}>
              <Button
                text="Create Label"
                color={ComponentColor.Success}
                type={ButtonType.Submit}
                status={ComponentStatus.Default}
                onClick={this.handleSubmit}
              />
            </ComponentSpacer>
          </Grid.Column>
        </Grid.Row>
      </Grid>
    )
  }

  private handleSubmit = (e: FormEvent) => {
    e.preventDefault()

    this.props.onSubmit(this.state.label)
  }

  private handleCustomColorChange = (
    e: ChangeEvent<HTMLInputElement>
  ): void => {
    this.updateProperties({color: e.target.value})
  }

  private handleColorChange = (color: string) => {
    this.updateProperties({color})
  }

  private handleInputChange = (e: ChangeEvent<HTMLInputElement>): void => {
    this.updateProperties({description: e.target.value})
  }

  private handleToggleCustomColorHex = (isCustomColor: boolean) => {
    this.setState({isCustomColor})
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

  private get dropdownColorHex(): string {
    if (this.state.isCustomColor) {
      return CUSTOM_LABEL.colorHex
    }

    return this.colorHex
  }

  private get customColorInput(): JSX.Element {
    const {colorHex} = this

    if (!this.state.isCustomColor) {
      return null
    }

    return (
      <Form.ValidationElement
        label="Enter a Hexcode"
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
            autoFocus={true}
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
