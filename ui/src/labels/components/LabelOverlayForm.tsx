// Libraries
import React, {PureComponent, ChangeEvent} from 'react'

// Components
import {
  Input,
  Button,
  ColorPicker,
  Grid,
  Label,
  Form,
} from '@influxdata/clockface'

// Types
import {
  Columns,
  InputType,
  ButtonType,
  ComponentSize,
  ComponentColor,
  ComponentStatus,
} from '@influxdata/clockface'

// Constants
import {INPUT_ERROR_COLOR} from 'src/labels/constants'
const MAX_LABEL_CHARS = 50

// Utils
import {validateHexCode} from 'src/labels/utils/'

// Decorators
import {ErrorHandling} from 'src/shared/decorators/errors'

interface Props {
  id: string
  name: string
  description: string
  color: string
  onColorChange: (color: string, status?: ComponentStatus) => void
  onSubmit: () => void
  onCloseModal: () => void
  onInputChange: (e: ChangeEvent<HTMLInputElement>) => void
  onLabelPropertyChange: (e: ChangeEvent<HTMLInputElement>) => void
  onNameValidation: (name: string) => string | null
  buttonText: string
  isFormValid: boolean
}

@ErrorHandling
export default class LabelOverlayForm extends PureComponent<Props> {
  public render() {
    const {
      id,
      name,
      color,
      onSubmit,
      buttonText,
      description,
      onCloseModal,
      onInputChange,
      onLabelPropertyChange,
      onColorChange,
      isFormValid,
    } = this.props

    return (
      <Form onSubmit={onSubmit} testID="label-overlay-form">
        <Grid>
          <Grid.Row>
            <Grid.Column widthXS={Columns.Twelve}>
              <Form.Element label="Preview">
                <Form.Box className="label-overlay--preview">
                  <Label
                    size={ComponentSize.Small}
                    name={this.placeholderLabelName}
                    description={description}
                    color={this.validatedColor}
                    id={id}
                  />
                </Form.Box>
              </Form.Element>
            </Grid.Column>
            <Grid.Column widthSM={Columns.Twelve}>
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
                    testID="create-label-form--name"
                  />
                )}
              </Form.ValidationElement>
            </Grid.Column>
            <Grid.Column widthXS={Columns.Twelve}>
              <Form.Element label="Description">
                <Input
                  type={InputType.Text}
                  placeholder="Add a optional description"
                  name="description"
                  value={description}
                  onChange={onLabelPropertyChange}
                  testID="create-label-form--description"
                />
              </Form.Element>
            </Grid.Column>
            <Grid.Column widthSM={Columns.Twelve}>
              <Form.Element label="Color">
                <ColorPicker color={color} onChange={onColorChange} />
              </Form.Element>
            </Grid.Column>
            <Grid.Column widthXS={Columns.Twelve}>
              <Form.Footer>
                <Button
                  text="Cancel"
                  onClick={onCloseModal}
                  titleText="Cancel creation of Label and return to list"
                  type={ButtonType.Button}
                  testID="create-label-form--cancel"
                />
                <Button
                  text={buttonText}
                  color={ComponentColor.Success}
                  type={ButtonType.Submit}
                  testID="create-label-form--submit"
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

  private get validatedColor(): string {
    const {color} = this.props

    if (validateHexCode(color)) {
      return INPUT_ERROR_COLOR
    }

    return color
  }

  private handleNameValidation = (name: string): string | null => {
    return this.props.onNameValidation(name)
  }
}
