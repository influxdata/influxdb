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
  updateTelegrafPluginConfig,
  addConfigValue,
  removeConfigValue,
  setActiveTelegrafPlugin,
  setPluginConfiguration,
  createTelegrafConfigAsync,
  addPluginBundleWithPlugins,
  removePluginBundleWithPlugins,
  setConfigArrayValue,
} from 'src/onboarding/actions/dataLoaders'

// Constants
import {StepStatus} from 'src/clockface/constants/wizard'

// Types
import {Links} from 'src/types/v2/links'
import {SetupParams} from 'src/onboarding/apis'
import {DataLoadersState, DataLoaderType} from 'src/types/v2/dataLoaders'
import {Notification, NotificationFunc} from 'src/types'
import {AppState} from 'src/types/v2'
import OnboardingSideBar from 'src/onboarding/components/OnboardingSideBar'

export interface OnboardingStepProps {
  links: Links
  currentStepIndex: number
  onSetCurrentStepIndex: (stepNumber: number) => void
  onIncrementCurrentStepIndex: () => void
  onDecrementCurrentStepIndex: () => void
  onSetStepStatus: (index: number, status: StepStatus) => void
  onSetSubstepIndex: (index: number, subStep: number | 'streaming') => void
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
  onSetSubstepIndex: (stepNumber: number, substep: number | 'streaming') => void
}

interface DispatchProps {
  notify: (message: Notification | NotificationFunc) => void
  onSetSetupParams: typeof setSetupParams
  onSetStepStatus: typeof setStepStatus
  onSetDataLoadersType: typeof setDataLoadersType
  onAddPluginBundle: typeof addPluginBundleWithPlugins
  onRemovePluginBundle: typeof removePluginBundleWithPlugins
  onUpdateTelegrafPluginConfig: typeof updateTelegrafPluginConfig
  onAddConfigValue: typeof addConfigValue
  onRemoveConfigValue: typeof removeConfigValue
  onSetActiveTelegrafPlugin: typeof setActiveTelegrafPlugin
  onSetPluginConfiguration: typeof setPluginConfiguration
  onSaveTelegrafConfig: typeof createTelegrafConfigAsync
  onSetConfigArrayValue: typeof setConfigArrayValue
}

interface StateProps {
  links: Links
  stepStatuses: StepStatus[]
  setupParams: SetupParams
  dataLoaders: DataLoadersState
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
      dataLoaders: {telegrafPlugins, telegrafConfigID},
      onSetDataLoadersType,
      onSetActiveTelegrafPlugin,
      onSetPluginConfiguration,
      onUpdateTelegrafPluginConfig,
      onAddConfigValue,
      onRemoveConfigValue,
      onSaveTelegrafConfig,
      onAddPluginBundle,
      onRemovePluginBundle,
      setupParams,
      notify,
      onSetConfigArrayValue,
    } = this.props

    return (
      <WizardFullScreen>
        {this.progressHeader}
        <div className="wizard-contents">
          <OnboardingSideBar
            notify={notify}
            telegrafPlugins={telegrafPlugins}
            telegrafConfigID={telegrafConfigID}
            onTabClick={this.handleClickSideBarTab}
            title="Plugins to Configure"
            visible={this.sideBarVisible}
            currentStepIndex={currentStepIndex}
            onNewSourceClick={this.handleNewSourceClick}
          />
          <div className="wizard-step--container">
            <OnboardingStepSwitcher
              currentStepIndex={currentStepIndex}
              onboardingStepProps={this.onboardingStepProps}
              setupParams={setupParams}
              dataLoaders={dataLoaders}
              onSetDataLoadersType={onSetDataLoadersType}
              onUpdateTelegrafPluginConfig={onUpdateTelegrafPluginConfig}
              onSetActiveTelegrafPlugin={onSetActiveTelegrafPlugin}
              onSetPluginConfiguration={onSetPluginConfiguration}
              onAddConfigValue={onAddConfigValue}
              onRemoveConfigValue={onRemoveConfigValue}
              onSaveTelegrafConfig={onSaveTelegrafConfig}
              onAddPluginBundle={onAddPluginBundle}
              onRemovePluginBundle={onRemovePluginBundle}
              onSetConfigArrayValue={onSetConfigArrayValue}
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

  private handleNewSourceClick = () => {
    const {onSetSubstepIndex, onSetActiveTelegrafPlugin} = this.props

    onSetActiveTelegrafPlugin('')
    onSetSubstepIndex(2, 'streaming')
  }

  private handleClickSideBarTab = (telegrafPluginID: string) => {
    const {
      onSetSubstepIndex,
      onSetActiveTelegrafPlugin,
      dataLoaders: {telegrafPlugins},
    } = this.props

    const index = Math.max(
      _.findIndex(telegrafPlugins, plugin => {
        return plugin.name === telegrafPluginID
      }),
      0
    )

    onSetSubstepIndex(3, index)
    onSetActiveTelegrafPlugin(telegrafPluginID)
  }

  private handleExit = () => {
    const {router, onCompleteSetup} = this.props
    onCompleteSetup()
    router.push(`/`)
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
      onSetSubstepIndex,
      onDecrementCurrentStepIndex,
      onIncrementCurrentStepIndex,
    } = this.props

    return {
      stepStatuses,
      stepTitles: this.stepTitles,
      currentStepIndex,
      onSetCurrentStepIndex,
      onSetSubstepIndex,
      onIncrementCurrentStepIndex,
      onDecrementCurrentStepIndex,
      onSetStepStatus,
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
  onUpdateTelegrafPluginConfig: updateTelegrafPluginConfig,
  onAddConfigValue: addConfigValue,
  onRemoveConfigValue: removeConfigValue,
  onSetActiveTelegrafPlugin: setActiveTelegrafPlugin,
  onSaveTelegrafConfig: createTelegrafConfigAsync,
  onAddPluginBundle: addPluginBundleWithPlugins,
  onRemovePluginBundle: removePluginBundleWithPlugins,
  onSetPluginConfiguration: setPluginConfiguration,
  onSetConfigArrayValue: setConfigArrayValue,
}

export default connect<StateProps, DispatchProps, OwnProps>(
  mstp,
  mdtp
)(withRouter(OnboardingWizard))
