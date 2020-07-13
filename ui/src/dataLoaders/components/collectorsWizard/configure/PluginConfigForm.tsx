// Libraries
import React, {PureComponent} from 'react'
import {connect, ConnectedProps} from 'react-redux'
import _ from 'lodash'

// Components
import {Form, DapperScrollbars} from '@influxdata/clockface'
import ConfigFieldHandler from 'src/dataLoaders/components/collectorsWizard/configure/ConfigFieldHandler'

// Actions
import {
  setActiveTelegrafPlugin,
  setPluginConfiguration,
} from 'src/dataLoaders/actions/dataLoaders'

// Types
import {TelegrafPlugin, ConfigFields} from 'src/types/dataLoaders'
import OnboardingButtons from 'src/onboarding/components/OnboardingButtons'
import {AppState} from 'src/types'

interface OwnProps {
  telegrafPlugin: TelegrafPlugin
  configFields: ConfigFields
}

type ReduxProps = ConnectedProps<typeof connector>
type Props = OwnProps & ReduxProps

export class PluginConfigForm extends PureComponent<Props> {
  public render() {
    const {configFields, telegrafPlugin} = this.props
    return (
      <Form onSubmit={this.handleSubmitForm} className="data-loading--form">
        <DapperScrollbars
          autoHide={false}
          className="data-loading--scroll-content"
        >
          <div>
            <h3 className="wizard-step--title">
              {_.startCase(telegrafPlugin.name)}
            </h3>
            <h5 className="wizard-step--sub-title">
              For more information about this plugin, see{' '}
              <a
                target="_blank"
                data-testid="docs-link"
                href={`https://github.com/influxdata/telegraf/tree/master/plugins/inputs/${telegrafPlugin.name}`}
              >
                Documentation
              </a>
            </h5>
          </div>
          <ConfigFieldHandler
            configFields={configFields}
            telegrafPlugin={telegrafPlugin}
          />
        </DapperScrollbars>
        <OnboardingButtons
          autoFocusNext={this.autoFocus}
          nextButtonText="Done"
          className="data-loading--button-container"
        />
      </Form>
    )
  }

  private get autoFocus(): boolean {
    const {configFields} = this.props
    return !configFields
  }

  private handleSubmitForm = () => {
    const {
      telegrafPlugins,
      onSetPluginConfiguration,
      onSetActiveTelegrafPlugin,
    } = this.props

    const activeTelegrafPlugin = telegrafPlugins.find(tp => tp.active)
    if (!!activeTelegrafPlugin) {
      if (!activeTelegrafPlugin.hasOwnProperty('plugin')) {
        onSetActiveTelegrafPlugin('')
        return
      }
      onSetPluginConfiguration(activeTelegrafPlugin.name)
    }

    onSetActiveTelegrafPlugin('')
  }
}

const mstp = ({
  dataLoading: {
    dataLoaders: {telegrafPlugins},
  },
}: AppState) => ({
  telegrafPlugins,
})

const mdtp = {
  onSetActiveTelegrafPlugin: setActiveTelegrafPlugin,
  onSetPluginConfiguration: setPluginConfiguration,
}

const connector = connect(mstp, mdtp)

export default connector(PluginConfigForm)
