// Libraries
import React, {PureComponent} from 'react'
import _ from 'lodash'

// Components
import {ErrorHandling} from 'src/shared/decorators/errors'
import LineProtocol from 'src/onboarding/components/configureStep/lineProtocol/LineProtocol'
import PluginConfigSwitcher from 'src/onboarding/components/configureStep/streaming/PluginConfigSwitcher'
import EmptyDataSourceState from 'src/onboarding/components/configureStep/EmptyDataSourceState'

// Actions
import {
  updateTelegrafPluginConfig,
  setPluginConfiguration,
  addConfigValue,
  removeConfigValue,
} from 'src/onboarding/actions/dataLoaders'

// Types
import {TelegrafPlugin, DataLoaderType} from 'src/types/v2/dataLoaders'

export interface Props {
  telegrafPlugins: TelegrafPlugin[]
  currentIndex: number
  onUpdateTelegrafPluginConfig: typeof updateTelegrafPluginConfig
  onSetPluginConfiguration: typeof setPluginConfiguration
  onAddConfigValue: typeof addConfigValue
  onRemoveConfigValue: typeof removeConfigValue
  dataLoaderType: DataLoaderType
  authToken: string
  bucket: string
  org: string
  username: string
}

@ErrorHandling
class ConfigureDataSourceSwitcher extends PureComponent<Props> {
  public render() {
    const {
      bucket,
      org,
      authToken,
      telegrafPlugins,
      currentIndex,
      dataLoaderType,
      onUpdateTelegrafPluginConfig,
      onSetPluginConfiguration,
      onAddConfigValue,
      onRemoveConfigValue,
    } = this.props

    switch (dataLoaderType) {
      case DataLoaderType.Streaming:
        return (
          <PluginConfigSwitcher
            onUpdateTelegrafPluginConfig={onUpdateTelegrafPluginConfig}
            onSetPluginConfiguration={onSetPluginConfiguration}
            onRemoveConfigValue={onRemoveConfigValue}
            telegrafPlugins={telegrafPlugins}
            currentIndex={currentIndex}
            onAddConfigValue={onAddConfigValue}
            authToken={authToken}
          />
        )
      case DataLoaderType.LineProtocol:
        return <LineProtocol bucket={bucket} org={org} />
      case DataLoaderType.CSV:
        return <div>{DataLoaderType.CSV}</div>
      default:
        return <EmptyDataSourceState />
    }
  }
}

export default ConfigureDataSourceSwitcher
