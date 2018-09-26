import React, {PureComponent} from 'react'
import _ from 'lodash'

import InitStep from 'src/onboarding/components/InitStep'
import AdminStep from 'src/onboarding/components/AdminStep'
import CompletionStep from 'src/onboarding/components/CompletionStep'
import OtherStep from 'src/onboarding/components/OtherStep'
import {ErrorHandling} from 'src/shared/decorators/errors'
import {StepStatus} from 'src/clockface/constants/wizard'
import {
  WizardFullScreen,
  WizardProgressHeader,
  ProgressBar,
} from 'src/clockface'

interface Props {
  startStep?: number
  stepStatuses?: StepStatus[]
}

interface State {
  currentStepIndex: number
  stepStatuses: StepStatus[]
}

@ErrorHandling
class OnboardingWizard extends PureComponent<Props, State> {
  public static defaultProps: Partial<Props> = {
    startStep: 0,
    stepStatuses: [
      StepStatus.Incomplete,
      StepStatus.Incomplete,
      StepStatus.Incomplete,
      StepStatus.Incomplete,
      StepStatus.Incomplete,
    ],
  }

  public stepTitles = [
    'Welcome',
    'Setup admin',
    'Select data source',
    'Configure data source',
    'Complete',
  ]
  public steps = [InitStep, AdminStep, OtherStep, OtherStep, CompletionStep]

  constructor(props) {
    super(props)
    this.state = {
      currentStepIndex: props.startStep,
      stepStatuses: props.stepStatuses,
    }
  }

  public render() {
    const {stepStatuses} = this.props
    const {currentStepIndex} = this.state
    return (
      <WizardFullScreen>
        <WizardProgressHeader>
          <ProgressBar
            currentStepIndex={currentStepIndex}
            handleSetCurrentStep={this.onSetCurrentStep}
            stepStatuses={stepStatuses}
            stepTitles={this.stepTitles}
          />
        </WizardProgressHeader>
        <div className="wizard-step--container">{this.currentStep}</div>
      </WizardFullScreen>
    )
  }

  private get currentStep() {
    const {currentStepIndex} = this.state
    const {stepStatuses} = this.props

    return React.createElement(this.steps[currentStepIndex], {
      stepStatuses,
      stepTitles: this.stepTitles,
      currentStepIndex,
      handleSetCurrentStep: this.onSetCurrentStep,
      handleSetStepStatus: this.onSetStepStatus,
    })
  }

  private onSetCurrentStep = stepNumber => {
    this.setState({currentStepIndex: stepNumber})
  }

  private onSetStepStatus = (index: number, status: StepStatus) => {
    const {stepStatuses} = this.state
    stepStatuses[index] = status
    this.setState({stepStatuses})
  }
}

export default OnboardingWizard
