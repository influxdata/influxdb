// Libraries
import React, {Component, ChangeEvent} from 'react'

// Components
import LabelOverlayForm from 'src/configuration/components/LabelOverlayForm'
import {OverlayContainer, OverlayBody, OverlayHeading} from 'src/clockface'

// Utils
import {validateHexCode} from 'src/configuration/utils/labels'

// Types
import {LabelType} from 'src/clockface'

// Decorators
import {ErrorHandling} from 'src/shared/decorators/errors'

interface Props {
  label: LabelType
  onDismiss: () => void
  onUpdateLabel: (label: LabelType) => Promise<void>
  onNameValidation: (name: string) => string | null
}

interface State {
  label: LabelType
  useCustomColorHex: boolean
}

@ErrorHandling
class UpdateLabelOverlay extends Component<Props, State> {
  constructor(props: Props) {
    super(props)

    this.state = {
      label: props.label,
      useCustomColorHex: false,
    }
  }

  public render() {
    const {onDismiss, onNameValidation} = this.props
    const {label, useCustomColorHex} = this.state

    return (
      <OverlayContainer maxWidth={600}>
        <OverlayHeading title="Edit Label" onDismiss={onDismiss} />
        <OverlayBody>
          <LabelOverlayForm
            id={label.id}
            name={label.name}
            description={label.description}
            colorHex={label.colorHex}
            onColorHexChange={this.handleColorHexChange}
            onToggleCustomColorHex={this.handleToggleCustomColorHex}
            useCustomColorHex={useCustomColorHex}
            onSubmit={this.handleSubmit}
            onCloseModal={onDismiss}
            onInputChange={this.handleInputChange}
            buttonText="Save Changes"
            isFormValid={this.isFormValid}
            onNameValidation={onNameValidation}
          />
        </OverlayBody>
      </OverlayContainer>
    )
  }

  private get isFormValid(): boolean {
    const {label} = this.state

    const nameIsValid = this.props.onNameValidation(label.name) === null
    const colorIsValid = validateHexCode(label.colorHex) === null

    return nameIsValid && colorIsValid
  }

  private handleSubmit = () => {
    const {onUpdateLabel, onDismiss} = this.props

    onUpdateLabel(this.state.label)
    onDismiss()
  }

  private handleInputChange = (e: ChangeEvent<HTMLInputElement>): void => {
    const value = e.target.value
    const key = e.target.name

    if (key in this.state.label) {
      const label = {...this.state.label, [key]: value}

      this.setState({
        label,
      })
    }
  }

  private handleColorHexChange = (colorHex: string): void => {
    const label = {...this.state.label, colorHex}

    this.setState({label})
  }

  private handleToggleCustomColorHex = (useCustomColorHex: boolean): void => {
    this.setState({useCustomColorHex})
  }
}

export default UpdateLabelOverlay
