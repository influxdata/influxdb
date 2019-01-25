// Libraries
import React, {PureComponent} from 'react'
import _ from 'lodash'

// Components
import {ErrorHandling} from 'src/shared/decorators/errors'
import ConfigureDataSourceSwitcher from 'src/dataLoaders/components/configureStep/ConfigureDataSourceSwitcher'

// Actions
import {setActiveTelegrafPlugin} from 'src/dataLoaders/actions/dataLoaders'
import {
  updateTelegrafPluginConfig,
  setPluginConfiguration,
  addConfigValue,
  removeConfigValue,
  setConfigArrayValue,
} from 'src/dataLoaders/actions/dataLoaders'

// Types
import {DataLoaderStepProps} from 'src/dataLoaders/components/DataLoadersWizard'
import {TelegrafPlugin, DataLoaderType} from 'src/types/v2/dataLoaders'
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
        substepIndex={substep}
        onSetConfigArrayValue={onSetConfigArrayValue}
        onClickNext={this.handleNext}
        onClickPrevious={this.handlePrevious}
      />
    )
  }

  private handleNext = async () => {
    const {
      onIncrementCurrentStepIndex,
      onSetActiveTelegrafPlugin,
      onSetPluginConfiguration,
      telegrafPlugins,
      substep,
      currentStepIndex,
      type,
      onExit,
      onSetSubstepIndex,
    } = this.props

    if (type === DataLoaderType.Scraping) {
      onExit()
      return
    }

    if (type === DataLoaderType.Streaming && !_.isNaN(Number(substep))) {
      const index = +substep
      const telegrafPlugin = _.get(telegrafPlugins, `${index}.name`)
      onSetPluginConfiguration(telegrafPlugin)
      onSetActiveTelegrafPlugin('')
      onSetSubstepIndex(currentStepIndex, 'config')
      return
    }

    onIncrementCurrentStepIndex()
  }

  private handlePrevious = () => {
    const {
      type,
      substep,
      currentStepIndex,
      onSetSubstepIndex,
      onDecrementCurrentStepIndex,
    } = this.props

    if (type === DataLoaderType.Streaming) {
      if (substep === 'config') {
        onSetSubstepIndex(+currentStepIndex - 1, 'streaming')
      }
      return
    }

    onDecrementCurrentStepIndex()
  }
}

export default ConfigureDataSourceStep
