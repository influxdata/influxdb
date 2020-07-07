// Libraries
import React, {PureComponent} from 'react'
import {connect, ConnectedProps} from 'react-redux'

// Components
import PluginConfigForm from 'src/dataLoaders/components/collectorsWizard/configure/PluginConfigForm'
import EmptyDataSourceState from 'src/dataLoaders/components/configureStep/EmptyDataSourceState'

// Utils
import {getConfigFields} from 'src/dataLoaders/utils/pluginConfigs'

// Types
import {TelegrafPlugin, ConfigFields} from 'src/types/dataLoaders'
import {AppState} from 'src/types'
import TelegrafPluginInstructions from 'src/dataLoaders/components/collectorsWizard/configure/TelegrafPluginInstructions'

type ReduxProps = ConnectedProps<typeof connector>
type Props = ReduxProps

export class PluginConfigSwitcher extends PureComponent<Props> {
  public render() {
    const {telegrafPlugins} = this.props

    if (this.activeTelegrafPlugin) {
      return (
        <PluginConfigForm
          telegrafPlugin={this.activeTelegrafPlugin}
          configFields={this.configFields}
        />
      )
    } else if (!telegrafPlugins || !telegrafPlugins.length) {
      return <EmptyDataSourceState />
    }

    return <TelegrafPluginInstructions />
  }

  private get activeTelegrafPlugin(): TelegrafPlugin {
    const {telegrafPlugins} = this.props
    return telegrafPlugins.find(tp => tp.active)
  }

  private get configFields(): ConfigFields {
    if (this.activeTelegrafPlugin) {
      return getConfigFields(this.activeTelegrafPlugin.name)
    }
  }
}

const mstp = ({
  dataLoading: {
    dataLoaders: {telegrafPlugins},
  },
}: AppState) => ({
  telegrafPlugins,
})

const connector = connect(mstp)

export default connector(PluginConfigSwitcher)
