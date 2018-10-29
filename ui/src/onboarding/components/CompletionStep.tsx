// Libraries
import React, {PureComponent} from 'react'

// Components
import {ErrorHandling} from 'src/shared/decorators/errors'

// Types
import {Button, ComponentColor, ComponentSize} from 'src/clockface'
import {OnboardingStepProps} from 'src/onboarding/containers/OnboardingWizard'

@ErrorHandling
class CompletionStep extends PureComponent<OnboardingStepProps> {
  public componentDidMount() {
    window.addEventListener('keydown', this.handleKeydown)
  }
  public componentWillUnmount() {
    window.removeEventListener('keydown', this.handleKeydown)
  }
  public render() {
    const {onExit} = this.props
    return (
      <div className="onboarding-step">
        <div className="splash-logo secondary" />
        <h3 className="wizard-step--title">Setup Complete!</h3>
        <h5 className="wizard-step--sub-title" />
        <div className="wizard-button-bar">
          <Button
            color={ComponentColor.Default}
            text="Back"
            size={ComponentSize.Medium}
            onClick={this.handleDecrement}
          />
          <Button
            color={ComponentColor.Success}
            text="Go to InfluxDB 2.0"
            size={ComponentSize.Medium}
            onClick={onExit}
          />
        </div>
      </div>
    )
  }

  private handleDecrement = () => {
    const {handleSetCurrentStep, currentStepIndex} = this.props
    handleSetCurrentStep(currentStepIndex - 1)
  }

  private handleKeydown = (e: KeyboardEvent): void => {
    if (e.key === 'Enter') {
      this.props.onExit()
    }
  }
}

export default CompletionStep
