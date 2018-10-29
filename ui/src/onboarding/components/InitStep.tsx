// Libraries
import React, {PureComponent} from 'react'
// Components
import {ErrorHandling} from 'src/shared/decorators/errors'

// Types
import {StepStatus} from 'src/clockface/constants/wizard'
import {Button, ComponentColor, ComponentSize} from 'src/clockface'
import {OnboardingStepProps} from 'src/onboarding/containers/OnboardingWizard'

@ErrorHandling
class InitStep extends PureComponent<OnboardingStepProps> {
  public componentDidMount() {
    window.addEventListener('keydown', this.handleKeydown)
  }
  public componentWillUnmount() {
    window.removeEventListener('keydown', this.handleKeydown)
  }
  public render() {
    return (
      <div className="onboarding-step">
        <div className="splash-logo primary" />
        <h3 className="wizard-step--title">Welcome to InfluxDB 2.0</h3>
        <h5 className="wizard-step--sub-title">
          Get started in just a few easy steps
        </h5>
        <div className="wizard-button-bar">
          <Button
            color={ComponentColor.Primary}
            text="Get Started"
            size={ComponentSize.Large}
            onClick={this.handleNext}
          />
        </div>
      </div>
    )
  }

  private handleNext = () => {
    const {handleSetStepStatus, currentStepIndex} = this.props
    handleSetStepStatus(currentStepIndex, StepStatus.Complete)
    this.handleIncrement()
  }

  private handleIncrement = () => {
    const {handleSetCurrentStep, currentStepIndex} = this.props
    handleSetCurrentStep(currentStepIndex + 1)
  }

  private handleKeydown = (e: KeyboardEvent): void => {
    if (e.key === 'Enter') {
      this.handleNext()
    }
  }
}

export default InitStep
