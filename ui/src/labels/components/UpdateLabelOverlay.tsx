// Libraries
import React, {Component, ChangeEvent} from 'react'

// Components
import LabelOverlayForm from 'src/labels/components/LabelOverlayForm'
import {ComponentStatus, Overlay} from '@influxdata/clockface'

// Types
import {Label} from 'src/types'

// Decorators
import {ErrorHandling} from 'src/shared/decorators/errors'

interface Props {
  label: Label
  onDismiss: () => void
  onUpdateLabel: (label: Label) => void
  onNameValidation: (name: string) => string | null
}

interface State {
  label: Label
  colorStatus: ComponentStatus
}

@ErrorHandling
class UpdateLabelOverlay extends Component<Props, State> {
  constructor(props: Props) {
    super(props)

    this.state = {
      label: props.label,
      colorStatus: ComponentStatus.Default,
    }
  }

  public render() {
    const {onDismiss, onNameValidation} = this.props
    const {label} = this.state

    return (
      <Overlay.Container maxWidth={400}>
        <Overlay.Header title="Edit Label" onDismiss={onDismiss} />
        <Overlay.Body>
          <LabelOverlayForm
            id={label.id}
            name={label.name}
            description={label.properties.description}
            color={label.properties.color}
            onColorChange={this.handleColorHexChange}
            onSubmit={this.handleSubmit}
            onCloseModal={onDismiss}
            onInputChange={this.handleInputChange}
            onLabelPropertyChange={this.handleLabelPropertyChange}
            buttonText="Save Changes"
            isFormValid={this.isFormValid}
            onNameValidation={onNameValidation}
          />
        </Overlay.Body>
      </Overlay.Container>
    )
  }

  private get isFormValid(): boolean {
    const {label, colorStatus} = this.state

    const nameIsValid = this.props.onNameValidation(label.name) === null
    const colorIsValid =
      colorStatus === ComponentStatus.Default ||
      colorStatus == ComponentStatus.Valid

    return nameIsValid && colorIsValid
  }

  private handleSubmit = () => {
    const {onUpdateLabel, onDismiss} = this.props

    onUpdateLabel(this.state.label)
    onDismiss()
  }

  private handleInputChange = (e: ChangeEvent<HTMLInputElement>): void => {
    const {value, name} = e.target

    this.setState(prevState => ({
      label: {...prevState.label, [name]: value},
    }))
  }

  private handleLabelPropertyChange = (
    e: ChangeEvent<HTMLInputElement>
  ): void => {
    const {value, name} = e.target

    this.setState(prevState => ({
      label: {
        ...prevState.label,
        properties: {...prevState.label.properties, [name]: value},
      },
    }))
  }

  private handleColorHexChange = (
    color: string,
    colorStatus: ComponentStatus
  ): void => {
    const properties = {...this.state.label.properties, color}
    const label = {...this.state.label, properties}

    this.setState({label, colorStatus})
  }
}

export default UpdateLabelOverlay
