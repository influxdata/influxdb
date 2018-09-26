// Libraries
import React, {PureComponent} from 'react'
// Components
import {ErrorHandling} from 'src/shared/decorators/errors'

// Types
import {StepStatus} from 'src/clockface/constants/wizard'
import {Button, ComponentColor, ComponentSize} from 'src/clockface'

interface Props {
  currentStepIndex: number
  handleSetCurrentStep: (stepNumber: number) => void
  handleSetStepStatus: (index: number, status: StepStatus) => void
  stepStatuses: StepStatus[]
  stepTitles: string[]
}

@ErrorHandling
class InitStep extends PureComponent<Props> {
  public render() {
    return (
      <>
        <h3 className="wizard-step-title">Other step</h3>
        <p>This is a dummy step</p>
        <div className="wizard-button-bar">
          <Button
            color={ComponentColor.Default}
            text="Back"
            size={ComponentSize.Medium}
            onClick={this.handleDecrement}
          />
          <Button
            color={ComponentColor.Primary}
            text="Next"
            size={ComponentSize.Medium}
            onClick={this.handleNext}
          />
        </div>
      </>
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

  private handleDecrement = () => {
    const {handleSetCurrentStep, currentStepIndex} = this.props
    handleSetCurrentStep(currentStepIndex - 1)
  }
}

export default InitStep
