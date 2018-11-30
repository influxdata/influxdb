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
} from 'src/onboarding/actions/steps'
import {
  setDataLoadersType,
  addDataSource,
  removeDataSource,
} from 'src/onboarding/actions/dataLoaders'

// Constants
import {StepStatus} from 'src/clockface/constants/wizard'

// Types
import {Links} from 'src/types/v2/links'
import {SetupParams} from 'src/onboarding/apis'
import {DataSource, DataSourceType} from 'src/types/v2/dataSources'
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
  onSetDataLoadersType: typeof setDataLoadersType
  onAddDataSource: typeof addDataSource
  onRemoveDataSource: typeof removeDataSource
}

interface DataLoadersProps {
  dataSources: DataSource[]
  type: DataSourceType
}

interface StateProps {
  links: Links
  currentStepIndex: number
  stepStatuses: StepStatus[]
  setupParams: SetupParams
  dataLoaders: DataLoadersProps
}

type Props = OwnProps & StateProps & DispatchProps & WithRouterProps

@ErrorHandling
class OnboardingWizard extends PureComponent<Props> {
  public stepTitles = [
    'Welcome',
    'Admin Setup',
    'Select Data Sources',
    'Configure Data Sources',
    'Complete',
  ]

  public stepSkippable = [false, false, false, false, false]

  constructor(props: Props) {
    super(props)
  }

  public render() {
    const {
      currentStepIndex,
      dataLoaders,
      onSetDataLoadersType,
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
            dataLoaders={dataLoaders}
            onSetDataLoadersType={onSetDataLoadersType}
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
  onboarding: {
    steps: {currentStepIndex, stepStatuses, setupParams},
    dataLoaders,
  },
}: AppState): StateProps => ({
  links,
  currentStepIndex,
  stepStatuses,
  setupParams,
  dataLoaders,
})

const mdtp: DispatchProps = {
  notify: notifyAction,
  onSetSetupParams: setSetupParams,
  onDecrementCurrentStepIndex: decrementCurrentStepIndex,
  onIncrementCurrentStepIndex: incrementCurrentStepIndex,
  onSetCurrentStepIndex: setCurrentStepIndex,
  onSetStepStatus: setStepStatus,
  onSetDataLoadersType: setDataLoadersType,
  onAddDataSource: addDataSource,
  onRemoveDataSource: removeDataSource,
}

export default connect<StateProps, DispatchProps, OwnProps>(
  mstp,
  mdtp
)(withRouter(OnboardingWizard))
