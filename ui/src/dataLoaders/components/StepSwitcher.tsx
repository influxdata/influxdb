// Libraries
import React, {PureComponent} from 'react'
import _ from 'lodash'

// Components
import SelectDataSourceStep from 'src/dataLoaders/components/selectionStep/SelectDataSourceStep'
import ConfigureDataSourceStep from 'src/onboarding/components/configureStep/ConfigureDataSourceStep'
import VerifyDataStep from 'src/onboarding/components/verifyStep/VerifyDataStep'
import {ErrorHandling} from 'src/shared/decorators/errors'

// Actions
import {
  updateTelegrafPluginConfig,
  setDataLoadersType,
  setActiveTelegrafPlugin,
  addConfigValue,
  removeConfigValue,
  addPluginBundleWithPlugins,
  removePluginBundleWithPlugins,
  setPluginConfiguration,
  setConfigArrayValue,
} from 'src/dataLoaders/actions/dataLoaders'
import {setBucketInfo} from 'src/dataLoaders/actions/steps'

// Types
import {DataLoadersState, DataLoaderStep} from 'src/types/v2/dataLoaders'
import {DataLoaderStepProps} from 'src/dataLoaders/components/DataLoadersWizard'
import {Bucket} from 'src/api'

interface Props {
  onboardingStepProps: DataLoaderStepProps
  onUpdateTelegrafPluginConfig: typeof updateTelegrafPluginConfig
  onAddConfigValue: typeof addConfigValue
  onRemoveConfigValue: typeof removeConfigValue
  onSetDataLoadersType: typeof setDataLoadersType
  onSetActiveTelegrafPlugin: typeof setActiveTelegrafPlugin
  onSetPluginConfiguration: typeof setPluginConfiguration
  bucketName: string
  buckets: Bucket[]
  dataLoaders: DataLoadersState
  currentStepIndex: number
  onAddPluginBundle: typeof addPluginBundleWithPlugins
  onRemovePluginBundle: typeof removePluginBundleWithPlugins
  onSetConfigArrayValue: typeof setConfigArrayValue
  onSetBucketInfo: typeof setBucketInfo
  org: string
  username: string
  selectedBucket: string
}

@ErrorHandling
class StepSwitcher extends PureComponent<Props> {
  public render() {
    const {
      currentStepIndex,
      onboardingStepProps,
      dataLoaders,
      onSetDataLoadersType,
      onUpdateTelegrafPluginConfig,
      onSetActiveTelegrafPlugin,
      onSetPluginConfiguration,
      onAddConfigValue,
      onRemoveConfigValue,
      onAddPluginBundle,
      onRemovePluginBundle,
      onSetConfigArrayValue,
      bucketName,
      username,
      org,
      buckets,
      onSetBucketInfo,
      selectedBucket,
    } = this.props

    switch (currentStepIndex) {
      case DataLoaderStep.Select:
        return (
          <SelectDataSourceStep
            {...onboardingStepProps}
            {...dataLoaders}
            onSetDataLoadersType={onSetDataLoadersType}
            buckets={buckets}
            bucket={bucketName}
            selectedBucket={selectedBucket}
            onSetActiveTelegrafPlugin={onSetActiveTelegrafPlugin}
            onAddPluginBundle={onAddPluginBundle}
            onRemovePluginBundle={onRemovePluginBundle}
            onSetBucketInfo={onSetBucketInfo}
          />
        )
      case DataLoaderStep.Configure:
        return (
          <ConfigureDataSourceStep
            {...onboardingStepProps}
            {...dataLoaders}
            buckets={buckets}
            bucket={bucketName}
            username={username}
            org={org}
            onUpdateTelegrafPluginConfig={onUpdateTelegrafPluginConfig}
            onSetPluginConfiguration={onSetPluginConfiguration}
            onAddConfigValue={onAddConfigValue}
            onRemoveConfigValue={onRemoveConfigValue}
            onSetActiveTelegrafPlugin={onSetActiveTelegrafPlugin}
            onSetConfigArrayValue={onSetConfigArrayValue}
          />
        )
      case DataLoaderStep.Verify:
        return (
          <VerifyDataStep
            {...onboardingStepProps}
            {...dataLoaders}
            bucket={bucketName}
            selectedBucket={selectedBucket}
            username={username}
            org={org}
            onSetActiveTelegrafPlugin={onSetActiveTelegrafPlugin}
            onSetPluginConfiguration={onSetPluginConfiguration}
            stepIndex={currentStepIndex}
          />
        )
      default:
        return <div />
    }
  }
}

export default StepSwitcher
