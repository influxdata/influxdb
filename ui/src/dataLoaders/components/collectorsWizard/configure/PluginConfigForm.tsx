// Libraries
import React, {PureComponent} from 'react'
import {connect} from 'react-redux'
import _ from 'lodash'

// Components
import {Form} from '@influxdata/clockface'
import ConfigFieldHandler from 'src/dataLoaders/components/collectorsWizard/configure/ConfigFieldHandler'
import FancyScrollbar from 'src/shared/components/fancy_scrollbar/FancyScrollbar'
import PluginsSideBar from 'src/dataLoaders/components/collectorsWizard/configure/PluginsSideBar'

// Actions
import {
  setActiveTelegrafPlugin,
  setPluginConfiguration,
} from 'src/dataLoaders/actions/dataLoaders'

// Types
import {TelegrafPlugin, ConfigFields} from 'src/types/dataLoaders'
import OnboardingButtons from 'src/onboarding/components/OnboardingButtons'
import {AppState} from 'src/types'

// Selectors
import {getDataLoaders} from 'src/dataLoaders/selectors'

interface OwnProps {
  telegrafPlugin: TelegrafPlugin
  configFields: ConfigFields
}

interface DispatchProps {
  onSetActiveTelegrafPlugin: typeof setActiveTelegrafPlugin
  onSetPluginConfiguration: typeof setPluginConfiguration
}

interface StateProps {
  telegrafPlugins: TelegrafPlugin[]
}

type Props = OwnProps & StateProps & DispatchProps

export class PluginConfigForm extends PureComponent<Props> {
  public render() {
    const {configFields, telegrafPlugin, telegrafPlugins} = this.props
    return (
      <Form onSubmit={this.handleSubmitForm} className="data-loading--form">
        <div className="data-loading--scroll-content">
          <div>
            <h3 className="wizard-step--title">
              {_.startCase(telegrafPlugin.name)}
            </h3>
            <h5 className="wizard-step--sub-title">
              For more information about this plugin, see{' '}
              <a
                target="_blank"
                data-testid="docs-link"
                href={`https://github.com/influxdata/telegraf/tree/master/plugins/inputs/${
                  telegrafPlugin.name
                }`}
              >
                Documentation
              </a>
            </h5>
          </div>
          <div className="data-loading--columns">
            <PluginsSideBar
              telegrafPlugins={telegrafPlugins}
              onTabClick={this.handleClickSideBarTab}
              title="Plugins"
              visible
            />
            <div className="data-loading--column-panel">
              <FancyScrollbar
                autoHide={false}
                className="data-loading--scroll-content"
              >
                <ConfigFieldHandler
                  configFields={configFields}
                  telegrafPlugin={telegrafPlugin}
                />
              </FancyScrollbar>
            </div>
          </div>
        </div>
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
    const {onSetActiveTelegrafPlugin} = this.props

    onSetActiveTelegrafPlugin('')
  }

  private handleClickSideBarTab = (tabID: string) => {
    const {
      onSetActiveTelegrafPlugin,
      telegrafPlugins,
      onSetPluginConfiguration,
    } = this.props

    const activeTelegrafPlugin = telegrafPlugins.find(tp => tp.active)
    if (!!activeTelegrafPlugin) {
      onSetPluginConfiguration(activeTelegrafPlugin.name)
    }

    onSetActiveTelegrafPlugin(tabID)
  }
}

const mstp = (state: AppState): StateProps => {
  const {telegrafPlugins} = getDataLoaders(state)

  return {
    telegrafPlugins,
  }
}

const mdtp: DispatchProps = {
  onSetActiveTelegrafPlugin: setActiveTelegrafPlugin,
  onSetPluginConfiguration: setPluginConfiguration,
}

export default connect<StateProps, DispatchProps, OwnProps>(
  mstp,
  mdtp
)(PluginConfigForm)
