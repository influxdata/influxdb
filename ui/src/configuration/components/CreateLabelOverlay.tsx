// Libraries
import React, {Component, ChangeEvent} from 'react'

// Components
import LabelOverlayForm from 'src/configuration/components/LabelOverlayForm'
import {
  OverlayTechnology,
  OverlayContainer,
  OverlayBody,
  OverlayHeading,
} from 'src/clockface'

// Utils
import {validateHexCode} from 'src/configuration/utils/labels'

// Types
import {LabelType} from 'src/clockface'
import {Label as LabelAPI} from 'src/types/v2/labels'

// Constants
import {generateEmptyLabel} from 'src/configuration/constants/LabelColors'

// Decorators
import {ErrorHandling} from 'src/shared/decorators/errors'

interface Props {
  isVisible: boolean
  onDismiss: () => void
  onCreateLabel: (label: LabelAPI) => void
  onNameValidation: (name: string) => string | null
  overrideDefaultName?: string
}

interface State {
  label: LabelType
  useCustomColorHex: boolean
}

@ErrorHandling
class CreateLabelOverlay extends Component<Props, State> {
  constructor(props: Props) {
    super(props)

    this.state = {
      label: generateEmptyLabel(this.props.overrideDefaultName),
      useCustomColorHex: false,
    }
  }

  componentDidUpdate(prevProps) {
    if (prevProps.overrideDefaultName !== this.props.overrideDefaultName) {
      const label = {...this.state.label, name: this.props.overrideDefaultName}
      this.setState({label})
    }
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
      onCreateLabel(this.handleConstructLabel())
      // clear form on successful submit
      this.resetForm()
    } finally {
      onDismiss()
    }
  }

  private handleConstructLabel = (): LabelAPI => {
    const {label} = this.state

    return {
      name: label.name,
      id: label.id,
      properties: {
        color: label.colorHex,
        description: label.description,
      },
    }
  }

  private resetForm() {
    this.setState({
      label: generateEmptyLabel(),
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
