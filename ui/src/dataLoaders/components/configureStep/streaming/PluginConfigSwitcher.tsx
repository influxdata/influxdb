// Libraries
import React, {PureComponent} from 'react'
import _ from 'lodash'

// Components
import PluginConfigForm from 'src/dataLoaders/components/configureStep/streaming/PluginConfigForm'
import EmptyDataSourceState from 'src/dataLoaders/components/configureStep/EmptyDataSourceState'

// Utils
import {getConfigFields} from 'src/dataLoaders/utils/pluginConfigs'

// Actions
import {
  updateTelegrafPluginConfig,
  addConfigValue,
  removeConfigValue,
  setConfigArrayValue,
} from 'src/dataLoaders/actions/dataLoaders'

// Types
import {TelegrafPlugin, Substep} from 'src/types/v2/dataLoaders'
import TelegrafPluginInstructions from 'src/dataLoaders/components/configureStep/streaming/TelegrafPluginInstructions'

interface Props {
  telegrafPlugins: TelegrafPlugin[]
  onUpdateTelegrafPluginConfig: typeof updateTelegrafPluginConfig
  onAddConfigValue: typeof addConfigValue
  onRemoveConfigValue: typeof removeConfigValue
  substepIndex: Substep
  onSetConfigArrayValue: typeof setConfigArrayValue
  onClickNext: () => void
  onClickPrevious: () => void
}

class PluginConfigSwitcher extends PureComponent<Props> {
  public render() {
    const {
      onUpdateTelegrafPluginConfig,
      onAddConfigValue,
      onRemoveConfigValue,
      onSetConfigArrayValue,
      onClickNext,
      telegrafPlugins,
      substepIndex,
      onClickPrevious,
    } = this.props

    if (substepIndex === 'config') {
      return (
        <TelegrafPluginInstructions
          onClickNext={onClickNext}
          onClickPrevious={onClickPrevious}
        />
      )
    } else if (this.currentTelegrafPlugin) {
      return (
        <PluginConfigForm
          telegrafPlugin={this.currentTelegrafPlugin}
          onUpdateTelegrafPluginConfig={onUpdateTelegrafPluginConfig}
          configFields={this.configFields}
          onAddConfigValue={onAddConfigValue}
          onRemoveConfigValue={onRemoveConfigValue}
          onSetConfigArrayValue={onSetConfigArrayValue}
          onClickNext={onClickNext}
          telegrafPlugins={telegrafPlugins}
        />
      )
    } else {
      return <EmptyDataSourceState />
    }
  }

  private get currentTelegrafPlugin(): TelegrafPlugin {
    const {substepIndex, telegrafPlugins} = this.props
    return _.get(telegrafPlugins, `${substepIndex}`, null)
  }

  private get configFields() {
    return getConfigFields(this.currentTelegrafPlugin.name)
  }
}

export default PluginConfigSwitcher
