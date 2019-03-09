// Libraries
import React, {Component, ChangeEvent} from 'react'

// Components
import LabelOverlayForm from 'src/configuration/components/LabelOverlayForm'
import {
  OverlayTechnology,
  OverlayContainer,
  OverlayBody,
  OverlayHeading,
  ComponentStatus,
} from 'src/clockface'

// Types
import {ILabel} from '@influxdata/influx'

// Constants
import {EMPTY_LABEL} from 'src/configuration/constants/LabelColors'

// Decorators
import {ErrorHandling} from 'src/shared/decorators/errors'

interface Props {
  isVisible: boolean
  onDismiss: () => void
  onCreateLabel: (label: ILabel) => void
  onNameValidation: (name: string) => string | null
  overrideDefaultName?: string
}

interface State {
  label: ILabel
  colorStatus: ComponentStatus
}

@ErrorHandling
class CreateLabelOverlay extends Component<Props, State> {
  constructor(props: Props) {
    super(props)

    this.state = {
      label: {...EMPTY_LABEL, name: this.props.overrideDefaultName},
      colorStatus: ComponentStatus.Default,
    }
  }

  componentDidUpdate(prevProps) {
    if (
      prevProps.overrideDefaultName !== this.props.overrideDefaultName &&
      this.props.isVisible === false
    ) {
      const name = this.props.overrideDefaultName
      const label = {...this.state.label, name}

      this.setState({label})
    }
  }

  public render() {
    const {isVisible, onDismiss, onNameValidation} = this.props
    const {label} = this.state

    return (
      <OverlayTechnology visible={isVisible}>
        <OverlayContainer maxWidth={400}>
          <OverlayHeading title="Create Label" onDismiss={onDismiss} />
          <OverlayBody>
            <LabelOverlayForm
              id={label.id}
              name={label.name}
              description={label.properties.description}
              color={label.properties.color}
              onColorChange={this.handleColorChange}
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
    const {label, colorStatus} = this.state

    const nameIsValid = this.props.onNameValidation(label.name) === null
    const colorIsValid =
      colorStatus === ComponentStatus.Default ||
      colorStatus === ComponentStatus.Valid

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
    })
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

  private handleColorChange = (
    color: string,
    colorStatus: ComponentStatus
  ): void => {
    const properties = {...this.state.label.properties, color}
    const label = {...this.state.label, properties}

    this.setState({label, colorStatus})
  }
}

export default CreateLabelOverlay
