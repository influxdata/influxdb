// Libraries
import React, {PureComponent, ChangeEvent} from 'react'
import {connect, ConnectedProps} from 'react-redux'
import {includes, get} from 'lodash'

// Components
import {Form, Input, DapperScrollbars} from '@influxdata/clockface'
import OnboardingButtons from 'src/onboarding/components/OnboardingButtons'
import PluginsSideBar from 'src/dataLoaders/components/collectorsWizard/configure/PluginsSideBar'

// Actions
import {
  setTelegrafConfigName,
  setTelegrafConfigDescription,
  setActiveTelegrafPlugin,
  setPluginConfiguration,
  createOrUpdateTelegrafConfigAsync,
} from 'src/dataLoaders/actions/dataLoaders'
import {
  incrementCurrentStepIndex,
  decrementCurrentStepIndex,
} from 'src/dataLoaders/actions/steps'
import {notify as notifyAction} from 'src/shared/actions/notifications'

// APIs
import {createDashboardFromTemplate as createDashboardFromTemplateAJAX} from 'src/templates/api'

// Constants
import {
  TelegrafDashboardCreated,
  TelegrafDashboardFailed,
} from 'src/shared/copy/notifications'

// Types
import {AppState, ConfigurationState} from 'src/types'
import {InputType, ComponentSize} from '@influxdata/clockface'
import {influxdbTemplateList} from 'src/templates/constants/defaultTemplates'

// Selectors
import {getOrg} from 'src/organizations/selectors'
import {getDataLoaders} from 'src/dataLoaders/selectors'

type ReduxProps = ConnectedProps<typeof connector>
type Props = ReduxProps

export class TelegrafPluginInstructions extends PureComponent<Props> {
  public render() {
    const {
      telegrafConfigName,
      telegrafConfigDescription,
      telegrafPlugins,
      onDecrementStep,
    } = this.props

    return (
      <Form onSubmit={this.handleFormSubmit} className="data-loading--form">
        <div className="data-loading--scroll-content">
          <div>
            <h3 className="wizard-step--title">Configure Plugins</h3>
            <h5 className="wizard-step--sub-title">
              Configure each plugin from the menu on the left. Some plugins do
              not require any configuration.
            </h5>
          </div>
          <div className="data-loading--columns">
            <PluginsSideBar
              telegrafPlugins={telegrafPlugins}
              onTabClick={this.handleClickSideBarTab}
              title="Plugins"
              visible={this.sideBarVisible}
            />
            <div className="data-loading--column-panel">
              <DapperScrollbars
                autoHide={false}
                className="data-loading--scroll-content"
              >
                <Form.Element label="Telegraf Configuration Name">
                  <Input
                    type={InputType.Text}
                    value={telegrafConfigName}
                    name="name"
                    onChange={this.handleNameInput}
                    titleText="Telegraf Configuration Name"
                    size={ComponentSize.Medium}
                    autoFocus={true}
                  />
                </Form.Element>
                <Form.Element label="Telegraf Configuration Description">
                  <Input
                    type={InputType.Text}
                    value={telegrafConfigDescription}
                    name="description"
                    onChange={this.handleDescriptionInput}
                    titleText="Telegraf Configuration Description"
                    size={ComponentSize.Medium}
                  />
                </Form.Element>
              </DapperScrollbars>
            </div>
          </div>
        </div>

        <OnboardingButtons
          onClickBack={onDecrementStep}
          nextButtonText="Create and Verify"
          className="data-loading--button-container"
        />
      </Form>
    )
  }

  private handleFormSubmit = () => {
    const {onSaveTelegrafConfig, telegrafConfigID} = this.props

    onSaveTelegrafConfig()

    if (!telegrafConfigID) {
      this.handleCreateDashboardsForPlugins()
    }

    this.props.onIncrementStep()
  }

  private async handleCreateDashboardsForPlugins() {
    const {notify, telegrafPlugins, orgID} = this.props
    try {
      const configuredPlugins = telegrafPlugins.filter(
        tp => tp.configured === ConfigurationState.Configured
      )

      const configuredPluginTemplateIdentifiers = configuredPlugins
        .map(t => t.templateID)
        .filter(t => t)

      const templatesToInstantiate = influxdbTemplateList.filter(t => {
        return includes(
          configuredPluginTemplateIdentifiers,
          get(t, 'meta.templateID')
        )
      })

      const pendingDashboards = templatesToInstantiate.map(t =>
        createDashboardFromTemplateAJAX(t, orgID)
      )

      const pendingDashboardNames = templatesToInstantiate.map(t =>
        t.meta.name.toLowerCase()
      )

      const dashboards = await Promise.all(pendingDashboards)

      if (dashboards.length) {
        notify(TelegrafDashboardCreated(pendingDashboardNames))
      }
    } catch (err) {
      notify(TelegrafDashboardFailed())
    }
  }

  private get sideBarVisible() {
    const {telegrafPlugins} = this.props

    return telegrafPlugins.length > 0
  }

  private handleNameInput = (e: ChangeEvent<HTMLInputElement>) => {
    this.props.onSetTelegrafConfigName(e.target.value)
  }

  private handleDescriptionInput = (e: ChangeEvent<HTMLInputElement>) => {
    this.props.onSetTelegrafConfigDescription(e.target.value)
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

const mstp = (state: AppState) => {
  const {
    telegrafConfigName,
    telegrafConfigDescription,
    telegrafPlugins,
    telegrafConfigID,
  } = getDataLoaders(state)

  const {id: orgID} = getOrg(state)

  return {
    telegrafConfigName,
    telegrafConfigDescription,
    telegrafPlugins,
    telegrafConfigID,
    orgID,
  }
}

const mdtp = {
  onSetTelegrafConfigName: setTelegrafConfigName,
  onSetTelegrafConfigDescription: setTelegrafConfigDescription,
  onIncrementStep: incrementCurrentStepIndex,
  onDecrementStep: decrementCurrentStepIndex,
  onSetActiveTelegrafPlugin: setActiveTelegrafPlugin,
  onSetPluginConfiguration: setPluginConfiguration,
  onSaveTelegrafConfig: createOrUpdateTelegrafConfigAsync,
  notify: notifyAction,
}

const connector = connect(mstp, mdtp)

export default connector(TelegrafPluginInstructions)
