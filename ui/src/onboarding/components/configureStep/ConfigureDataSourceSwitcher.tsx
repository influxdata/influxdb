// Libraries
import React, {PureComponent} from 'react'
import _ from 'lodash'

// Components
import {ErrorHandling} from 'src/shared/decorators/errors'
import LineProtocol from 'src/onboarding/components/configureStep/lineProtocol/LineProtocol'
import PluginConfigSwitcher from 'src/onboarding/components/configureStep/streaming/PluginConfigSwitcher'
import EmptyDataSourceState from 'src/onboarding/components/configureStep/EmptyDataSourceState'
import Scraping from 'src/onboarding/components/configureStep/Scraping'

// Actions
import {
  updateTelegrafPluginConfig,
  addConfigValue,
  removeConfigValue,
  setConfigArrayValue,
} from 'src/onboarding/actions/dataLoaders'

// Types
import {TelegrafPlugin, DataLoaderType} from 'src/types/v2/dataLoaders'
import {Bucket} from 'src/api'

export interface Props {
  telegrafPlugins: TelegrafPlugin[]
  currentIndex: number
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
  onClickSkip: () => void
}

@ErrorHandling
class ConfigureDataSourceSwitcher extends PureComponent<Props> {
  public render() {
    const {
      bucket,
      org,
      telegrafPlugins,
      currentIndex,
      dataLoaderType,
      onUpdateTelegrafPluginConfig,
      onAddConfigValue,
      onRemoveConfigValue,
      onSetConfigArrayValue,
      onClickNext,
      onClickPrevious,
      onClickSkip,
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
              currentIndex={currentIndex}
              onAddConfigValue={onAddConfigValue}
              onSetConfigArrayValue={onSetConfigArrayValue}
              onClickNext={onClickNext}
              onClickPrevious={onClickPrevious}
              onClickSkip={onClickSkip}
            />
          </div>
        )
      case DataLoaderType.LineProtocol:
        return (
          <div className="onboarding-step">
            <LineProtocol
              bucket={bucket}
              org={org}
              onClickNext={onClickNext}
              onClickBack={onClickPrevious}
            />
          </div>
        )
      case DataLoaderType.Scraping:
        return (
          <div className="onboarding-step">
            <Scraping
              onClickNext={onClickNext}
              onClickBack={onClickPrevious}
              buckets={buckets}
            />
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
