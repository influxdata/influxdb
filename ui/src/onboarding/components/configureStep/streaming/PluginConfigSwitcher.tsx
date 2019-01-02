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
  addConfigValue,
  removeConfigValue,
  setConfigArrayValue,
} from 'src/onboarding/actions/dataLoaders'

// Types
import {TelegrafPlugin} from 'src/types/v2/dataLoaders'

interface Props {
  telegrafPlugins: TelegrafPlugin[]
  onUpdateTelegrafPluginConfig: typeof updateTelegrafPluginConfig
  onAddConfigValue: typeof addConfigValue
  onRemoveConfigValue: typeof removeConfigValue
  currentIndex: number
  onSetConfigArrayValue: typeof setConfigArrayValue
  onClickNext: () => void
  onClickPrevious: () => void
  onClickSkip: () => void
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
      currentIndex,
      onClickPrevious,
      onClickSkip,
    } = this.props

    if (this.currentTelegrafPlugin) {
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
          currentIndex={currentIndex}
          onClickPrevious={onClickPrevious}
          onClickSkip={onClickSkip}
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
