// Libraries
import React, {PureComponent} from 'react'
import _ from 'lodash'

// Components
import {ErrorHandling} from 'src/shared/decorators/errors'
import LineProtocol from 'src/dataLoaders/components/configureStep/lineProtocol/LineProtocol'
import PluginConfigSwitcher from 'src/dataLoaders/components/configureStep/streaming/PluginConfigSwitcher'
import EmptyDataSourceState from 'src/dataLoaders/components/configureStep/EmptyDataSourceState'
import Scraping from 'src/dataLoaders/components/configureStep/Scraping'

// Actions
import {
  updateTelegrafPluginConfig,
  addConfigValue,
  removeConfigValue,
  setConfigArrayValue,
} from 'src/dataLoaders/actions/dataLoaders'

// Types
import {TelegrafPlugin, DataLoaderType, Substep} from 'src/types/v2/dataLoaders'
import {Bucket} from 'src/api'

export interface Props {
  telegrafPlugins: TelegrafPlugin[]
  substepIndex: Substep
  onUpdateTelegrafPluginConfig: typeof updateTelegrafPluginConfig
  onAddConfigValue: typeof addConfigValue
  onRemoveConfigValue: typeof removeConfigValue
  dataLoaderType: DataLoaderType
  buckets: Bucket[]
  bucket: string
  org: string
  username: string
  onSetConfigArrayValue: typeof setConfigArrayValue
  onClickNext: () => void
  onClickPrevious: () => void
}

@ErrorHandling
class ConfigureDataSourceSwitcher extends PureComponent<Props> {
  public render() {
    const {
      bucket,
      org,
      telegrafPlugins,
      substepIndex,
      dataLoaderType,
      onUpdateTelegrafPluginConfig,
      onAddConfigValue,
      onRemoveConfigValue,
      onSetConfigArrayValue,
      onClickNext,
      onClickPrevious,
      buckets,
    } = this.props

    switch (dataLoaderType) {
      case DataLoaderType.Streaming:
        return (
          <div className="onboarding-step wizard--skippable">
            <PluginConfigSwitcher
              onUpdateTelegrafPluginConfig={onUpdateTelegrafPluginConfig}
              onRemoveConfigValue={onRemoveConfigValue}
              telegrafPlugins={telegrafPlugins}
              substepIndex={substepIndex}
              onAddConfigValue={onAddConfigValue}
              onSetConfigArrayValue={onSetConfigArrayValue}
              onClickNext={onClickNext}
              onClickPrevious={onClickPrevious}
            />
          </div>
        )
      case DataLoaderType.LineProtocol:
        return (
          <div className="onboarding-step">
            <LineProtocol bucket={bucket} org={org} onClickNext={onClickNext} />
          </div>
        )
      case DataLoaderType.Scraping:
        return (
          <div className="onboarding-step">
            <Scraping onClickNext={onClickNext} buckets={buckets} />
          </div>
        )
      case DataLoaderType.CSV:
        return <div>{DataLoaderType.CSV}</div>
      default:
        return <EmptyDataSourceState />
    }
  }
}

export default ConfigureDataSourceSwitcher
