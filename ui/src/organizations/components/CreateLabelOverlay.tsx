// Libraries
import React, {Component, ChangeEvent} from 'react'

// Components
import LabelOverlayForm from 'src/organizations/components/LabelOverlayForm'
import {
  OverlayTechnology,
  OverlayContainer,
  OverlayBody,
  OverlayHeading,
} from 'src/clockface'

// Utils
import {validateHexCode} from 'src/organizations/utils/labels'

// Types
import {LabelType} from 'src/clockface'

// Constants
import {EMPTY_LABEL} from 'src/organizations/constants/LabelColors'

// Decorators
import {ErrorHandling} from 'src/shared/decorators/errors'

interface Props {
  isVisible: boolean
  onDismiss: () => void
  onCreateLabel: (label: LabelType) => void
  onNameValidation: (name: string) => string | null
}

interface State {
  label: LabelType
  useCustomColorHex: boolean
}

@ErrorHandling
class CreateLabelOverlay extends Component<Props, State> {
  public state: State = {
    label: EMPTY_LABEL,
    useCustomColorHex: false,
  }

  public render() {
    const {isVisible, onDismiss, onNameValidation} = this.props
    const {label, useCustomColorHex} = this.state

    return (
      <OverlayTechnology visible={isVisible}>
        <OverlayContainer maxWidth={600}>
          <OverlayHeading title="Create Label" onDismiss={onDismiss} />
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
              buttonText="Create Label"
              isFormValid={this.isFormValid}
              onNameValidation={onNameValidation}
            />
          </OverlayBody>
        </OverlayContainer>
      </OverlayTechnology>
    )
  }

  private get isFormValid(): boolean {
    const {label} = this.state

    const nameIsValid = this.props.onNameValidation(label.name) === null
    const colorIsValid = validateHexCode(label.colorHex) === null

    return nameIsValid && colorIsValid
  }

  private handleSubmit = () => {
    const {onCreateLabel, onDismiss} = this.props

    try {
      onCreateLabel(this.state.label)
      // clear form on successful submit
      this.resetForm()
    } finally {
      onDismiss()
    }
  }

  private resetForm() {
    this.setState({
      label: EMPTY_LABEL,
      useCustomColorHex: false,
    })
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

export default CreateLabelOverlay
