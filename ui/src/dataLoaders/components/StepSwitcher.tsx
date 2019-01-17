// Libraries
import React, {PureComponent} from 'react'
import _ from 'lodash'

// Components
import SelectDataSourceStep from 'src/onboarding/components/selectionStep/SelectDataSourceStep'
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
  createOrUpdateTelegrafConfigAsync,
  addPluginBundleWithPlugins,
  removePluginBundleWithPlugins,
  setPluginConfiguration,
  setConfigArrayValue,
} from 'src/onboarding/actions/dataLoaders'

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
  onSaveTelegrafConfig: typeof createOrUpdateTelegrafConfigAsync
  onAddPluginBundle: typeof addPluginBundleWithPlugins
  onRemovePluginBundle: typeof removePluginBundleWithPlugins
  onSetConfigArrayValue: typeof setConfigArrayValue
  org: string
  username: string
}

@ErrorHandling
class StepSwitcher extends PureComponent<Props> {
  public render() {
    const {
      currentStepIndex,
      onboardingStepProps,
      dataLoaders,
      onSetDataLoadersType,
      onSaveTelegrafConfig,
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
    } = this.props

    switch (currentStepIndex) {
      case DataLoaderStep.Select:
        return (
          <SelectDataSourceStep
            {...onboardingStepProps}
            {...dataLoaders}
            onSetDataLoadersType={onSetDataLoadersType}
            bucket={bucketName}
            onSetActiveTelegrafPlugin={onSetActiveTelegrafPlugin}
            onAddPluginBundle={onAddPluginBundle}
            onRemovePluginBundle={onRemovePluginBundle}
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
            username={username}
            org={org}
            onSaveTelegrafConfig={onSaveTelegrafConfig}
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
