// Libraries
import React, {PureComponent, ChangeEvent} from 'react'

// Components
import {
  Stack,
  Button,
  Columns,
  Alignment,
  ButtonType,
  ComponentSize,
  ComponentColor,
  ComponentStatus,
} from '@influxdata/clockface'
import {
  Grid,
  Form,
  Input,
  ComponentSpacer,
  Label,
  InputType,
} from 'src/clockface'
import LabelColorDropdown from 'src/configuration/components/LabelColorDropdown'

// Constants
import {
  CUSTOM_LABEL,
  HEX_CODE_CHAR_LENGTH,
  INPUT_ERROR_COLOR,
} from 'src/configuration/constants/LabelColors'
const MAX_LABEL_CHARS = 50

// Utils
import {validateHexCode} from 'src/configuration/utils/labels'

// Styles
import 'src/configuration/components/LabelOverlayForm.scss'

// Decorators
import {ErrorHandling} from 'src/shared/decorators/errors'

interface Props {
  id: string
  name: string
  description: string
  colorHex: string
  onSubmit: () => void
  onCloseModal: () => void
  onInputChange: (e: ChangeEvent<HTMLInputElement>) => void
  onColorHexChange: (colorHex: string) => void
  onToggleCustomColorHex: (useCustomColorHex: boolean) => void
  onNameValidation: (name: string) => string | null
  useCustomColorHex: boolean
  buttonText: string
  isFormValid: boolean
}

@ErrorHandling
export default class LabelOverlayForm extends PureComponent<Props> {
  public render() {
    const {
      id,
      name,
      onSubmit,
      buttonText,
      description,
      onCloseModal,
      onInputChange,
      onColorHexChange,
      useCustomColorHex,
      onToggleCustomColorHex,
      isFormValid,
    } = this.props

    return (
      <Form onSubmit={onSubmit}>
        <Grid>
          <Grid.Row>
            <Grid.Column widthXS={Columns.Twelve}>
              <Form.Element label="Preview">
                <Form.Box className="label-overlay--preview">
                  <Label
                    size={ComponentSize.Small}
                    name={this.placeholderLabelName}
                    description={description}
                    colorHex={this.colorHexGuard}
                    id={id}
                  />
                </Form.Box>
              </Form.Element>
            </Grid.Column>
            <Grid.Column widthSM={Columns.Seven}>
              <Form.ValidationElement
                label="Name"
                value={name}
                required={true}
                validationFunc={this.handleNameValidation}
              >
                {status => (
                  <Input
                    type={InputType.Text}
                    placeholder="Name this Label"
                    name="name"
                    autoFocus={true}
                    value={name}
                    onChange={onInputChange}
                    status={status}
                    maxLength={MAX_LABEL_CHARS}
                  />
                )}
              </Form.ValidationElement>
            </Grid.Column>
            <Grid.Column widthSM={Columns.Five}>
              <Form.Element label="Color">
                <ComponentSpacer
                  align={Alignment.Left}
                  stackChildren={Stack.Rows}
                >
                  <LabelColorDropdown
                    colorHex={this.dropdownColorHex}
                    onChange={onColorHexChange}
                    useCustomColorHex={useCustomColorHex}
                    onToggleCustomColorHex={onToggleCustomColorHex}
                  />
                  {this.customColorInput}
                </ComponentSpacer>
              </Form.Element>
            </Grid.Column>
            <Grid.Column widthXS={Columns.Twelve}>
              <Form.Element label="Description">
                <Input
                  type={InputType.Text}
                  placeholder="Add a optional description"
                  name="description"
                  value={description}
                  onChange={onInputChange}
                />
              </Form.Element>
            </Grid.Column>
            <Grid.Column widthXS={Columns.Twelve}>
              <Form.Footer>
                <Button
                  text="Cancel"
                  onClick={onCloseModal}
                  titleText="Cancel creation of Label and return to list"
                  type={ButtonType.Button}
                />
                <Button
                  text={buttonText}
                  color={ComponentColor.Success}
                  type={ButtonType.Submit}
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

  private get placeholderLabelName(): string {
    const {name} = this.props

    if (!name) {
      return 'Name this Label'
    }

    return name
  }

  private get colorHexGuard(): string {
    const {colorHex} = this.props

    if (validateHexCode(colorHex)) {
      return INPUT_ERROR_COLOR
    }

    return colorHex
  }

  private get dropdownColorHex(): string {
    const {colorHex, useCustomColorHex} = this.props

    if (useCustomColorHex) {
      return CUSTOM_LABEL.colorHex
    }

    return colorHex
  }

  private handleNameValidation = (name: string): string | null => {
    return this.props.onNameValidation(name)
  }

  private get customColorInput(): JSX.Element {
    const {colorHex, useCustomColorHex} = this.props

    if (!useCustomColorHex) {
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

  private handleCustomColorChange = (
    e: ChangeEvent<HTMLInputElement>
  ): void => {
    const {onColorHexChange} = this.props

    onColorHexChange(e.target.value)
  }
}
