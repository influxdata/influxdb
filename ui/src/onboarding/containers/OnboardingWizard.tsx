// Libraries
import React, {PureComponent} from 'react'
import {withRouter, WithRouterProps} from 'react-router'

import {connect} from 'react-redux'
import _ from 'lodash'

// Components
import InitStep from 'src/onboarding/components/InitStep'
import AdminStep from 'src/onboarding/components/AdminStep'
import OtherStep from 'src/onboarding/components/OtherStep'
import CompletionStep from 'src/onboarding/components/CompletionStep'
import {ErrorHandling} from 'src/shared/decorators/errors'
import {
  WizardFullScreen,
  WizardProgressHeader,
  ProgressBar,
} from 'src/clockface'

// Actions
import {notify as notifyAction} from 'src/shared/actions/notifications'

// Constants
import {StepStatus} from 'src/clockface/constants/wizard'

// Types
import {Links} from 'src/types/v2/links'
import {SetupParams} from 'src/onboarding/apis'
import {Notification, NotificationFunc} from 'src/types'

export interface OnboardingStepProps {
  links: Links
  currentStepIndex: number
  handleSetCurrentStep: (stepNumber: number) => void
  handleSetStepStatus: (index: number, status: StepStatus) => void
  stepStatuses: StepStatus[]
  stepTitles: string[]
  setupParams: SetupParams
  handleSetSetupParams: (setupParams: SetupParams) => void
  notify: (message: Notification | NotificationFunc) => void
  onCompleteSetup: () => void
  onExit: () => void
}

interface Props extends WithRouterProps {
  links: Links
  startStep?: number
  stepStatuses?: StepStatus[]
  notify: (message: Notification | NotificationFunc) => void
  onCompleteSetup: () => void
}

interface State {
  currentStepIndex: number
  stepStatuses: StepStatus[]
  setupParams: SetupParams
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
    ],
  }

  public stepTitles = ['Welcome', 'Admin Setup', 'Next Step', 'Complete']
  public steps = [InitStep, AdminStep, OtherStep, CompletionStep]
  public stepSkippable = [false, false, true, false]

  constructor(props: Props) {
    super(props)
    this.state = {
      currentStepIndex: props.startStep,
      stepStatuses: props.stepStatuses,
      setupParams: null,
    }
  }

  public render() {
    return (
      <WizardFullScreen>
        {this.progressHeader}
        <div className="wizard-step--container">{this.currentStep}</div>
      </WizardFullScreen>
    )
  }

  private get progressHeader(): JSX.Element {
    const {stepStatuses} = this.props
    const {currentStepIndex} = this.state

    if (
      currentStepIndex === 0 ||
      currentStepIndex === stepStatuses.length - 1
    ) {
      return <div className="wizard--progress-header hidden" />
    }

    return (
      <WizardProgressHeader
        currentStepIndex={currentStepIndex}
        stepSkippable={this.stepSkippable}
        onSkip={this.handleSkip}
      >
        <ProgressBar
          currentStepIndex={currentStepIndex}
          handleSetCurrentStep={this.onSetCurrentStep}
          stepStatuses={stepStatuses}
          stepTitles={this.stepTitles}
        />
      </WizardProgressHeader>
    )
  }

  private get currentStep(): React.ReactElement<OnboardingStepProps> {
    const {currentStepIndex, setupParams} = this.state
    const {stepStatuses, links, notify, onCompleteSetup} = this.props

    return React.createElement(this.steps[currentStepIndex], {
      stepStatuses,
      stepTitles: this.stepTitles,
      currentStepIndex,
      handleSetCurrentStep: this.onSetCurrentStep,
      handleSetStepStatus: this.onSetStepStatus,
      links,
      setupParams,
      handleSetSetupParams: this.onSetSetupParams,
      notify,
      onCompleteSetup,
      onExit: this.handleExit,
    })
  }

  private handleExit = () => {
    const {router, onCompleteSetup} = this.props
    onCompleteSetup()
    router.push(`/manage-sources`)
  }

  private handleSkip = () => {
    const {stepStatuses} = this.props
    this.setState({currentStepIndex: stepStatuses.length - 1})
  }

  private onSetSetupParams = (setupParams: SetupParams): void => {
    this.setState({setupParams})
  }

  private onSetCurrentStep = (stepNumber: number): void => {
    this.setState({currentStepIndex: stepNumber})
  }

  private onSetStepStatus = (index: number, status: StepStatus): void => {
    const {stepStatuses} = this.state
    stepStatuses[index] = status
    this.setState({stepStatuses})
  }
}

const mstp = ({links}) => ({
  links,
})

const mdtp = {
  notify: notifyAction,
}

export default connect(
  mstp,
  mdtp
)(withRouter(OnboardingWizard))
