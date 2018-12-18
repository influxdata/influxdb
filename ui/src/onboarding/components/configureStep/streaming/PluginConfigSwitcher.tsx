// Libraries
import React, {PureComponent} from 'react'
import _ from 'lodash'

// Components
import PluginConfigForm from 'src/onboarding/components/configureStep/streaming/PluginConfigForm'
import EmptyDataSourceState from 'src/onboarding/components/configureStep/EmptyDataSourceState'

// Utils
import {getConfigFields} from 'src/onboarding/utils/pluginConfigs'

// Actions
import {
  updateTelegrafPluginConfig,
  setPluginConfiguration,
  addConfigValue,
  removeConfigValue,
  setConfigArrayValue,
} from 'src/onboarding/actions/dataLoaders'

// Types
import {TelegrafPlugin} from 'src/types/v2/dataLoaders'

interface Props {
  telegrafPlugins: TelegrafPlugin[]
  onUpdateTelegrafPluginConfig: typeof updateTelegrafPluginConfig
  onSetPluginConfiguration: typeof setPluginConfiguration
  onAddConfigValue: typeof addConfigValue
  onRemoveConfigValue: typeof removeConfigValue
  currentIndex: number
  onSetConfigArrayValue: typeof setConfigArrayValue
}

class PluginConfigSwitcher extends PureComponent<Props> {
  public render() {
    const {
      onUpdateTelegrafPluginConfig,
      onSetPluginConfiguration,
      onAddConfigValue,
      onRemoveConfigValue,
      onSetConfigArrayValue,
    } = this.props

    if (this.currentTelegrafPlugin) {
      return (
        <PluginConfigForm
          telegrafPlugin={this.currentTelegrafPlugin}
          onUpdateTelegrafPluginConfig={onUpdateTelegrafPluginConfig}
          onSetPluginConfiguration={onSetPluginConfiguration}
          configFields={this.configFields}
          onAddConfigValue={onAddConfigValue}
          onRemoveConfigValue={onRemoveConfigValue}
          onSetConfigArrayValue={onSetConfigArrayValue}
        />
      )
    }
    return <EmptyDataSourceState />
  }

  private get currentTelegrafPlugin(): TelegrafPlugin {
    const {currentIndex, telegrafPlugins} = this.props
    return _.get(telegrafPlugins, `${currentIndex}`, null)
  }

  private get configFields() {
    return getConfigFields(this.currentTelegrafPlugin.name)
  }
}

export default PluginConfigSwitcher
