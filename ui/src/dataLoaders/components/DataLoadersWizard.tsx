// Libraries
import React, {PureComponent} from 'react'
import {connect} from 'react-redux'
import _ from 'lodash'

// Components
import {ErrorHandling} from 'src/shared/decorators/errors'
import {WizardProgressHeader, ProgressBar} from 'src/clockface'
import WizardOverlay from 'src/clockface/components/wizard/WizardOverlay'
import StepSwitcher from 'src/dataLoaders/components/StepSwitcher'

// Actions
import {notify as notifyAction} from 'src/shared/actions/notifications'
import {
  setBucketInfo,
  setStepStatus,
  incrementCurrentStepIndex,
  decrementCurrentStepIndex,
  setCurrentStepIndex,
  setSubstepIndex,
  clearSteps,
} from 'src/onboarding/actions/steps'

import {
  setDataLoadersType,
  updateTelegrafPluginConfig,
  addConfigValue,
  removeConfigValue,
  setActiveTelegrafPlugin,
  setPluginConfiguration,
  createOrUpdateTelegrafConfigAsync,
  addPluginBundleWithPlugins,
  removePluginBundleWithPlugins,
  setConfigArrayValue,
  clearDataLoaders,
} from 'src/onboarding/actions/dataLoaders'

// Constants
import {StepStatus} from 'src/clockface/constants/wizard'

// Types
import {Links} from 'src/types/v2/links'
import {
  DataLoadersState,
  DataLoaderType,
  Substep,
} from 'src/types/v2/dataLoaders'
import {Notification, NotificationFunc} from 'src/types'
import {AppState} from 'src/types/v2'
import {Bucket} from 'src/api'
import PluginsSideBar from 'src/onboarding/components/PluginsSideBar'

export interface DataLoaderStepProps {
  links: Links
  currentStepIndex: number
  substep: Substep
  onSetCurrentStepIndex: (stepNumber: number) => void
  onIncrementCurrentStepIndex: () => void
  onDecrementCurrentStepIndex: () => void
  onSetStepStatus: (index: number, status: StepStatus) => void
  onSetSubstepIndex: (index: number, subStep: number | 'streaming') => void
  stepStatuses: StepStatus[]
  stepTitles: string[]
  notify: (message: Notification | NotificationFunc) => void
  onCompleteSetup: () => void
  onExit: () => void
}

interface OwnProps {
  onCompleteSetup: () => void
  visible: boolean
  bucket?: Bucket
  buckets: Bucket[]
  startingType?: DataLoaderType
  startingStep?: number
  startingSubstep?: Substep
}

interface DispatchProps {
  notify: (message: Notification | NotificationFunc) => void
  onSetBucketInfo: typeof setBucketInfo
  onSetStepStatus: typeof setStepStatus
  onSetDataLoadersType: typeof setDataLoadersType
  onAddPluginBundle: typeof addPluginBundleWithPlugins
  onRemovePluginBundle: typeof removePluginBundleWithPlugins
  onUpdateTelegrafPluginConfig: typeof updateTelegrafPluginConfig
  onAddConfigValue: typeof addConfigValue
  onRemoveConfigValue: typeof removeConfigValue
  onSetActiveTelegrafPlugin: typeof setActiveTelegrafPlugin
  onSetPluginConfiguration: typeof setPluginConfiguration
  onSetConfigArrayValue: typeof setConfigArrayValue
  onSaveTelegrafConfig: typeof createOrUpdateTelegrafConfigAsync
  onIncrementCurrentStepIndex: typeof incrementCurrentStepIndex
  onDecrementCurrentStepIndex: typeof decrementCurrentStepIndex
  onSetCurrentStepIndex: typeof setCurrentStepIndex
  onSetSubstepIndex: typeof setSubstepIndex
  onClearDataLoaders: typeof clearDataLoaders
  onClearSteps: typeof clearSteps
}

interface StateProps {
  links: Links
  stepStatuses: StepStatus[]
  dataLoaders: DataLoadersState
  currentStepIndex: number
  substep: Substep
  username: string
}

type Props = OwnProps & StateProps & DispatchProps

@ErrorHandling
class DataLoadersWizard extends PureComponent<Props> {
  public stepTitles = [
    'Select Data Sources',
    'Configure Data Sources',
    'Verify',
  ]

  public stepSkippable = [true, true, true]

  public componentDidMount() {
    this.handleSetBucketInfo()
    this.handleSetStartingValues()
  }

  public componentDidUpdate(prevProps: Props) {
    const {bucket, buckets} = this.props

    const prevBucket = prevProps.bucket || prevProps.buckets[0]
    const curBucket = bucket || buckets[0]

    const prevID = _.get(prevBucket, 'id', '')
    const curID = _.get(curBucket, 'id', '')
    const isDifferentBucket = prevID !== curID

    if (isDifferentBucket && curBucket) {
      this.handleSetBucketInfo()
    }
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
      notify,
      onSetConfigArrayValue,
      visible,
      bucket,
      username,
      buckets,
    } = this.props

    return (
      <WizardOverlay
        visible={visible}
        title={'Data Loading'}
        onDismis={this.handleDismiss}
      >
        {this.progressHeader}
        <div className="wizard-contents">
          <PluginsSideBar
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
            <StepSwitcher
              currentStepIndex={currentStepIndex}
              onboardingStepProps={this.stepProps}
              bucketName={_.get(bucket, 'name', '')}
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
              org={_.get(bucket, 'organization', '')}
              username={username}
              buckets={buckets}
            />
          </div>
        </div>
      </WizardOverlay>
    )
  }

  private handleSetBucketInfo = () => {
    const {bucket, buckets} = this.props
    if (bucket || buckets.length) {
      const b = bucket || buckets[0]
      const {organization, organizationID, name, id} = b

      this.props.onSetBucketInfo(organization, organizationID, name, id)
    }
  }

  private handleSetStartingValues = () => {
    const {startingStep, startingType, startingSubstep} = this.props

    const hasStartingStep = startingStep || startingStep === 0
    const hasStartingSubstep = startingSubstep || startingSubstep === 0
    const hasStartingType =
      startingType || startingType === DataLoaderType.Empty

    if (hasStartingType) {
      this.props.onSetDataLoadersType(startingType)
    }

    if (hasStartingSubstep) {
      this.props.onSetSubstepIndex(
        hasStartingStep ? startingStep : 0,
        startingSubstep
      )
    } else if (hasStartingStep) {
      this.props.onSetCurrentStepIndex(startingStep)
    }
  }

  private handleDismiss = () => {
    this.props.onClearDataLoaders()
    this.props.onClearSteps()
    this.props.onCompleteSetup()
    this.handleSetStartingValues()
  }

  private get progressHeader(): JSX.Element {
    const {stepStatuses, currentStepIndex, onSetCurrentStepIndex} = this.props

    return (
      <WizardProgressHeader>
        <ProgressBar
          currentStepIndex={currentStepIndex}
          handleSetCurrentStep={onSetCurrentStepIndex}
          stepStatuses={stepStatuses}
          stepTitles={this.stepTitles}
          stepSkippable={this.stepSkippable}
        />
      </WizardProgressHeader>
    )
  }

  private get sideBarVisible() {
    const {dataLoaders} = this.props
    const {telegrafPlugins, type} = dataLoaders

    const isStreaming = type === DataLoaderType.Streaming
    const isNotEmpty = telegrafPlugins.length > 0

    return isStreaming && isNotEmpty
  }

  private handleNewSourceClick = () => {
    const {onSetSubstepIndex, onSetActiveTelegrafPlugin} = this.props

    onSetActiveTelegrafPlugin('')
    onSetSubstepIndex(0, 'streaming')
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

    onSetSubstepIndex(1, index)
    onSetActiveTelegrafPlugin(telegrafPluginID)
  }

  private get stepProps(): DataLoaderStepProps {
    const {
      stepStatuses,
      links,
      notify,
      substep,
      onCompleteSetup,
      currentStepIndex,
      onSetStepStatus,
      onSetCurrentStepIndex,
      onSetSubstepIndex,
      onDecrementCurrentStepIndex,
      onIncrementCurrentStepIndex,
    } = this.props

    return {
      stepStatuses,
      substep,
      stepTitles: this.stepTitles,
      currentStepIndex,
      onSetCurrentStepIndex,
      onSetSubstepIndex,
      onIncrementCurrentStepIndex,
      onDecrementCurrentStepIndex,
      onSetStepStatus,
      links,
      notify,
      onCompleteSetup,
      onExit: this.handleDismiss,
    }
  }
}

const mstp = ({
  links,
  dataLoading: {
    dataLoaders,
    steps: {stepStatuses, currentStep, substep},
  },
  me: {name},
}: AppState): StateProps => ({
  links,
  stepStatuses,
  dataLoaders,
  currentStepIndex: currentStep,
  substep,
  username: name,
})

const mdtp: DispatchProps = {
  notify: notifyAction,
  onSetBucketInfo: setBucketInfo,
  onSetStepStatus: setStepStatus,
  onSetDataLoadersType: setDataLoadersType,
  onUpdateTelegrafPluginConfig: updateTelegrafPluginConfig,
  onAddConfigValue: addConfigValue,
  onRemoveConfigValue: removeConfigValue,
  onSetActiveTelegrafPlugin: setActiveTelegrafPlugin,
  onSaveTelegrafConfig: createOrUpdateTelegrafConfigAsync,
  onAddPluginBundle: addPluginBundleWithPlugins,
  onRemovePluginBundle: removePluginBundleWithPlugins,
  onSetPluginConfiguration: setPluginConfiguration,
  onSetConfigArrayValue: setConfigArrayValue,
  onIncrementCurrentStepIndex: incrementCurrentStepIndex,
  onDecrementCurrentStepIndex: decrementCurrentStepIndex,
  onSetCurrentStepIndex: setCurrentStepIndex,
  onSetSubstepIndex: setSubstepIndex,
  onClearDataLoaders: clearDataLoaders,
  onClearSteps: clearSteps,
}

export default connect<StateProps, DispatchProps, OwnProps>(
  mstp,
  mdtp
)(DataLoadersWizard)
