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
import {setSetupParams, setStepStatus} from 'src/onboarding/actions/steps'

import {
  setDataLoadersType,
  addTelegrafPlugin,
  removeTelegrafPlugin,
  setActiveTelegrafPlugin,
} from 'src/onboarding/actions/dataLoaders'

// Constants
import {StepStatus} from 'src/clockface/constants/wizard'

// Types
import {Links} from 'src/types/v2/links'
import {SetupParams} from 'src/onboarding/apis'
import {TelegrafPlugin, DataLoaderType} from 'src/types/v2/dataLoaders'
import {Notification, NotificationFunc} from 'src/types'
import {AppState} from 'src/types/v2'
import OnboardingSideBar from 'src/onboarding/components/OnboardingSideBar'

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
  currentStepIndex: number
  onIncrementCurrentStepIndex: () => void
  onDecrementCurrentStepIndex: () => void
  onSetCurrentStepIndex: (stepNumber: number) => void
  onSetCurrentSubStepIndex: (stepNumber: number, substepNumber: number) => void
}

interface DispatchProps {
  notify: (message: Notification | NotificationFunc) => void
  onSetSetupParams: typeof setSetupParams
  onSetStepStatus: typeof setStepStatus
  onSetDataLoadersType: typeof setDataLoadersType
  onAddTelegrafPlugin: typeof addTelegrafPlugin
  onRemoveTelegrafPlugin: typeof removeTelegrafPlugin
  onSetActiveTelegrafPlugin: typeof setActiveTelegrafPlugin
}

interface DataLoadersProps {
  telegrafPlugins: TelegrafPlugin[]
  type: DataLoaderType
}

interface StateProps {
  links: Links
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
    'Verify',
    'Complete',
  ]

  public stepSkippable = [false, false, false, false, false, false]

  constructor(props: Props) {
    super(props)
  }

  public render() {
    const {
      currentStepIndex,
      dataLoaders,
      dataLoaders: {telegrafPlugins},
      onSetDataLoadersType,
      onRemoveTelegrafPlugin,
      onAddTelegrafPlugin,
      setupParams,
      notify,
    } = this.props

    return (
      <WizardFullScreen>
        {this.progressHeader}
        <div className="wizard-contents">
          <OnboardingSideBar
            notify={notify}
            telegrafPlugins={telegrafPlugins}
            onTabClick={this.handleClickSideBarTab}
            title="Selected Sources"
            visible={this.sideBarVisible}
          />
          <div className="wizard-step--container">
            <OnboardingStepSwitcher
              currentStepIndex={currentStepIndex}
              onboardingStepProps={this.onboardingStepProps}
              setupParams={setupParams}
              dataLoaders={dataLoaders}
              onSetDataLoadersType={onSetDataLoadersType}
              onAddTelegrafPlugin={onAddTelegrafPlugin}
              onRemoveTelegrafPlugin={onRemoveTelegrafPlugin}
            />
          </div>
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

    if (currentStepIndex === 0) {
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

  private get sideBarVisible() {
    const {currentStepIndex, dataLoaders} = this.props
    const {telegrafPlugins, type} = dataLoaders

    const isStreaming = type === DataLoaderType.Streaming
    const isNotEmpty = telegrafPlugins.length > 0
    const isSideBarStep =
      (currentStepIndex === 2 && isNotEmpty) ||
      currentStepIndex === 3 ||
      currentStepIndex === 4

    return isStreaming && isSideBarStep
  }

  private handleClickSideBarTab = (telegrafPluginID: string) => {
    const {
      onSetCurrentSubStepIndex,
      onSetActiveTelegrafPlugin,
      dataLoaders: {telegrafPlugins},
    } = this.props

    const index = Math.max(
      _.findIndex(telegrafPlugins, plugin => {
        return plugin.name === telegrafPluginID
      }),
      0
    )

    onSetCurrentSubStepIndex(3, index)
    onSetActiveTelegrafPlugin(telegrafPluginID)
  }

  private handleExit = () => {
    const {router, onCompleteSetup} = this.props
    onCompleteSetup()
    router.push(`/sources`)
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
}

const mstp = ({
  links,
  onboarding: {
    steps: {stepStatuses, setupParams},
    dataLoaders,
  },
}: AppState): StateProps => ({
  links,
  stepStatuses,
  setupParams,
  dataLoaders,
})

const mdtp: DispatchProps = {
  notify: notifyAction,
  onSetSetupParams: setSetupParams,
  onSetStepStatus: setStepStatus,
  onSetDataLoadersType: setDataLoadersType,
  onAddTelegrafPlugin: addTelegrafPlugin,
  onRemoveTelegrafPlugin: removeTelegrafPlugin,
  onSetActiveTelegrafPlugin: setActiveTelegrafPlugin,
}

export default connect<StateProps, DispatchProps, OwnProps>(
  mstp,
  mdtp
)(withRouter(OnboardingWizard))
