// Libraries
import React, {PureComponent} from 'react'
import {withRouter, WithRouterProps} from 'react-router'

import {connect} from 'react-redux'
import _ from 'lodash'

// Components
import {ErrorHandling} from 'src/shared/decorators/errors'
import {
  WizardFullScreen,
  WizardProgressHeader,
  ProgressBar,
} from 'src/clockface'
import OnboardingStepSwitcher from 'src/onboarding/components/OnboardingStepSwitcher'

// Actions
import {notify as notifyAction} from 'src/shared/actions/notifications'
import {
  setSetupParams,
  incrementCurrentStepIndex,
  decrementCurrentStepIndex,
  setCurrentStepIndex,
  setStepStatus,
  addDataSource,
  removeDataSource,
} from 'src/onboarding/actions'

// Constants
import {StepStatus} from 'src/clockface/constants/wizard'

// Types
import {Links} from 'src/types/v2/links'
import {SetupParams} from 'src/onboarding/apis'
import {DataSource} from 'src/types/v2/dataSources'
import {Notification, NotificationFunc} from 'src/types'
import {AppState} from 'src/types/v2'

export interface OnboardingStepProps {
  links: Links
  currentStepIndex: number
  onSetCurrentStepIndex: (stepNumber: number) => void
  onIncrementCurrentStepIndex: () => void
  onDecrementCurrentStepIndex: () => void
  handleSetStepStatus: (index: number, status: StepStatus) => void
  stepStatuses: StepStatus[]
  stepTitles: string[]
  setupParams: SetupParams
  handleSetSetupParams: (setupParams: SetupParams) => void
  notify: (message: Notification | NotificationFunc) => void
  onCompleteSetup: () => void
  onExit: () => void
}

interface OwnProps {
  startStep?: number
  stepStatuses?: StepStatus[]
  onCompleteSetup: () => void
}

interface DispatchProps {
  notify: (message: Notification | NotificationFunc) => void
  onSetSetupParams: typeof setSetupParams
  onIncrementCurrentStepIndex: typeof incrementCurrentStepIndex
  onDecrementCurrentStepIndex: typeof decrementCurrentStepIndex
  onSetCurrentStepIndex: typeof setCurrentStepIndex
  onSetStepStatus: typeof setStepStatus
  onAddDataSource: typeof addDataSource
  onRemoveDataSource: typeof removeDataSource
}

interface StateProps {
  links: Links
  currentStepIndex: number
  stepStatuses: StepStatus[]
  setupParams: SetupParams
  dataSources: DataSource[]
}

type Props = OwnProps & StateProps & DispatchProps & WithRouterProps

@ErrorHandling
class OnboardingWizard extends PureComponent<Props> {
  public stepTitles = [
    'Welcome',
    'Admin Setup',
    'Select Data Sources',
    'Next Step',
    'Complete',
  ]

  public stepSkippable = [false, false, false, true, false]

  constructor(props: Props) {
    super(props)
    this.state = {
      dataSources: [],
    }
  }

  public render() {
    const {
      currentStepIndex,
      dataSources,
      onRemoveDataSource,
      onAddDataSource,
      setupParams,
    } = this.props
    const currentStepTitle = this.stepTitles[currentStepIndex]

    return (
      <WizardFullScreen>
        {this.progressHeader}
        <div className="wizard-step--container">
          <OnboardingStepSwitcher
            onboardingStepProps={this.onboardingStepProps}
            stepTitle={currentStepTitle}
            setupParams={setupParams}
            dataSources={dataSources}
            onAddDataSource={onAddDataSource}
            onRemoveDataSource={onRemoveDataSource}
          />
        </div>
      </WizardFullScreen>
    )
  }

  private get progressHeader(): JSX.Element {
    const {
      stepStatuses,
      currentStepIndex,
      onIncrementCurrentStepIndex,
      onSetCurrentStepIndex,
    } = this.props

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
        onSkip={onIncrementCurrentStepIndex}
      >
        <ProgressBar
          currentStepIndex={currentStepIndex}
          handleSetCurrentStep={onSetCurrentStepIndex}
          stepStatuses={stepStatuses}
          stepTitles={this.stepTitles}
        />
      </WizardProgressHeader>
    )
  }

  private get onboardingStepProps(): OnboardingStepProps {
    const {
      stepStatuses,
      links,
      notify,
      onCompleteSetup,
      setupParams,
      currentStepIndex,
      onSetStepStatus,
      onSetSetupParams,
      onSetCurrentStepIndex,
      onDecrementCurrentStepIndex,
      onIncrementCurrentStepIndex,
    } = this.props

    return {
      stepStatuses,
      stepTitles: this.stepTitles,
      currentStepIndex,
      onSetCurrentStepIndex,
      onIncrementCurrentStepIndex,
      onDecrementCurrentStepIndex,
      handleSetStepStatus: onSetStepStatus,
      links,
      setupParams,
      handleSetSetupParams: onSetSetupParams,
      notify,
      onCompleteSetup,
      onExit: this.handleExit,
    }
  }

  private handleExit = () => {
    const {router, onCompleteSetup} = this.props
    onCompleteSetup()
    router.push(`/sources`)
  }
}

const mstp = ({
  links,
  onboarding: {currentStepIndex, stepStatuses, setupParams, dataSources},
}: AppState): StateProps => ({
  links,
  currentStepIndex,
  stepStatuses,
  setupParams,
  dataSources,
})

const mdtp: DispatchProps = {
  notify: notifyAction,
  onSetSetupParams: setSetupParams,
  onDecrementCurrentStepIndex: decrementCurrentStepIndex,
  onIncrementCurrentStepIndex: incrementCurrentStepIndex,
  onSetCurrentStepIndex: setCurrentStepIndex,
  onSetStepStatus: setStepStatus,
  onAddDataSource: addDataSource,
  onRemoveDataSource: removeDataSource,
}

export default connect<StateProps, DispatchProps, OwnProps>(
  mstp,
  mdtp
)(withRouter(OnboardingWizard))
