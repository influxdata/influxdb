// Libraries
import React, {PureComponent} from 'react'

// Components
import {ErrorHandling} from 'src/shared/decorators/errors'
import {Button, ComponentColor, ComponentSize} from 'src/clockface'

// Types
import {StepStatus} from 'src/clockface/constants/wizard'
import {OnboardingStepProps} from 'src/onboarding/containers/OnboardingWizard'

@ErrorHandling
class OtherStep extends PureComponent<OnboardingStepProps, null> {
  constructor(props) {
    super(props)
  }
  public componentDidMount() {
    window.addEventListener('keydown', this.handleKeydown)
  }
  public componentWillUnmount() {
    window.removeEventListener('keydown', this.handleKeydown)
  }
  public render() {
    return (
      <div className="onboarding-step">
        <h3 className="wizard-step--title">This is Another Step</h3>
        <h5 className="wizard-step--sub-title">Import data here</h5>
        <div className="wizard-button-bar">
          <Button
            color={ComponentColor.Default}
            text="Back"
            size={ComponentSize.Medium}
            onClick={this.handlePrevious}
          />
          <Button
            color={ComponentColor.Primary}
            text="Next"
            size={ComponentSize.Medium}
            onClick={this.handleNext}
            titleText={'Next'}
          />
        </div>
      </div>
    )
  }

  private handleNext = async () => {
    const {handleSetStepStatus, currentStepIndex} = this.props
    handleSetStepStatus(currentStepIndex, StepStatus.Complete)
    this.handleIncrement()
  }

  private handlePrevious = () => {
    this.handleDecrement()
  }

  private handleIncrement = () => {
    const {handleSetCurrentStep, currentStepIndex} = this.props
    handleSetCurrentStep(currentStepIndex + 1)
  }

  private handleDecrement = () => {
    const {handleSetCurrentStep, currentStepIndex} = this.props
    handleSetCurrentStep(currentStepIndex - 1)
  }

  private handleKeydown = (e: KeyboardEvent): void => {
    if (e.key === 'Enter') {
      this.handleNext()
    }
  }
}

export default OtherStep
