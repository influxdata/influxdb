// Libraries
import React, {PureComponent} from 'react'
import _ from 'lodash'

// Components
import {ErrorHandling} from 'src/shared/decorators/errors'
import ConfigureDataSourceSwitcher from 'src/onboarding/components/configureStep/ConfigureDataSourceSwitcher'

// Actions
import {setActiveTelegrafPlugin} from 'src/onboarding/actions/dataLoaders'
import {
  updateTelegrafPluginConfig,
  setPluginConfiguration,
  addConfigValue,
  removeConfigValue,
  setConfigArrayValue,
} from 'src/onboarding/actions/dataLoaders'

// Types
import {DataLoaderStepProps} from 'src/dataLoaders/components/DataLoadersWizard'
import {
  TelegrafPlugin,
  DataLoaderType,
  DataLoaderStep,
} from 'src/types/v2/dataLoaders'
import {Bucket} from 'src/api'

export interface OwnProps extends DataLoaderStepProps {
  telegrafPlugins: TelegrafPlugin[]
  onSetActiveTelegrafPlugin: typeof setActiveTelegrafPlugin
  onUpdateTelegrafPluginConfig: typeof updateTelegrafPluginConfig
  onSetPluginConfiguration: typeof setPluginConfiguration
  type: DataLoaderType
  onAddConfigValue: typeof addConfigValue
  onRemoveConfigValue: typeof removeConfigValue
  onSetConfigArrayValue: typeof setConfigArrayValue
  bucket: string
  org: string
  username: string
  buckets: Bucket[]
}

type Props = OwnProps

@ErrorHandling
export class ConfigureDataSourceStep extends PureComponent<Props> {
  constructor(props: Props) {
    super(props)
  }

  public render() {
    const {
      telegrafPlugins,
      type,
      substep,
      onUpdateTelegrafPluginConfig,
      onAddConfigValue,
      onRemoveConfigValue,
      onSetConfigArrayValue,
      bucket,
      org,
      username,
      buckets,
    } = this.props

    return (
      <ConfigureDataSourceSwitcher
        buckets={buckets}
        bucket={bucket}
        org={org}
        username={username}
        telegrafPlugins={telegrafPlugins}
        onUpdateTelegrafPluginConfig={onUpdateTelegrafPluginConfig}
        onAddConfigValue={onAddConfigValue}
        onRemoveConfigValue={onRemoveConfigValue}
        dataLoaderType={type}
        currentIndex={+substep}
        onSetConfigArrayValue={onSetConfigArrayValue}
        onClickNext={this.handleNext}
        onClickPrevious={this.handlePrevious}
        onClickSkip={this.jumpToCompletionStep}
      />
    )
  }

  private jumpToCompletionStep = () => {
    const {onSetCurrentStepIndex} = this.props

    onSetCurrentStepIndex(DataLoaderStep.Verify)
  }

  private handleNext = async () => {
    const {
      onIncrementCurrentStepIndex,
      onSetActiveTelegrafPlugin,
      onSetPluginConfiguration,
      telegrafPlugins,
      substep,
      currentStepIndex,
      onSetSubstepIndex,
      type,
      onExit,
    } = this.props

    const index = +substep
    const telegrafPlugin = _.get(telegrafPlugins, `${index}.name`)

    onSetPluginConfiguration(telegrafPlugin)

    if (type === DataLoaderType.Streaming) {
      if (index >= telegrafPlugins.length - 1) {
        onIncrementCurrentStepIndex()
        onSetActiveTelegrafPlugin('')
      } else {
        const name = _.get(telegrafPlugins, `${index + 1}.name`, '')
        onSetActiveTelegrafPlugin(name)
        onSetSubstepIndex(+currentStepIndex, index + 1)
      }
      return
    } else if (
      type === DataLoaderType.Scraping &&
      currentStepIndex === DataLoaderStep.Configure
    ) {
      onExit()
    }

    onIncrementCurrentStepIndex()
  }

  private handlePrevious = () => {
    const {
      type,
      substep,
      currentStepIndex,
      onSetActiveTelegrafPlugin,
      onSetPluginConfiguration,
      telegrafPlugins,
      onSetSubstepIndex,
      onDecrementCurrentStepIndex,
    } = this.props

    const index = +substep
    const telegrafPlugin = _.get(telegrafPlugins, `${index}.name`)

    if (type === DataLoaderType.Streaming) {
      onSetPluginConfiguration(telegrafPlugin)

      if (index > 0) {
        const name = _.get(telegrafPlugins, `${index - 1}.name`)
        onSetActiveTelegrafPlugin(name)
        onSetSubstepIndex(+currentStepIndex, index - 1)
      } else {
        onSetActiveTelegrafPlugin('')
        onSetSubstepIndex(+currentStepIndex - 1, 'streaming')
      }

      return
    }

    onDecrementCurrentStepIndex()
  }
}

export default ConfigureDataSourceStep
