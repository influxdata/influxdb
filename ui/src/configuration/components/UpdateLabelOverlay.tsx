// Libraries
import React, {Component, ChangeEvent} from 'react'

// Components
import LabelOverlayForm from 'src/configuration/components/LabelOverlayForm'
import {OverlayContainer, OverlayBody, OverlayHeading} from 'src/clockface'

// Utils
import {validateHexCode} from 'src/configuration/utils/labels'

// Types
import {ILabel} from '@influxdata/influx'

// Decorators
import {ErrorHandling} from 'src/shared/decorators/errors'

interface Props {
  label: ILabel
  onDismiss: () => void
  onUpdateLabel: (label: ILabel) => Promise<void>
  onNameValidation: (name: string) => string | null
}

interface State {
  label: ILabel
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
            description={label.properties.description}
            colorHex={label.properties.color}
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
    const colorIsValid = validateHexCode(label.properties.color) === null

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

    if (key === 'description' || key === 'color') {
      const properties = {...this.state.label.properties, [key]: value}
      const label = {...this.state.label, properties}

      this.setState({
        label,
      })
    } else {
      const label = {...this.state.label, [key]: value}

      this.setState({
        label,
      })
    }
  }

  private handleColorHexChange = (color: string): void => {
    const properties = {...this.state.label.properties, color}
    const label = {...this.state.label, properties}

    this.setState({label})
  }

  private handleToggleCustomColorHex = (useCustomColorHex: boolean): void => {
    this.setState({useCustomColorHex})
  }
}

export default UpdateLabelOverlay
