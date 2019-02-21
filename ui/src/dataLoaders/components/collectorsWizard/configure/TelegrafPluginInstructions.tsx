// Libraries
import React, {PureComponent, ChangeEvent} from 'react'
import {connect} from 'react-redux'

// Components
import {Form, Input, InputType, ComponentSize} from 'src/clockface'
import FancyScrollbar from 'src/shared/components/fancy_scrollbar/FancyScrollbar'
import OnboardingButtons from 'src/onboarding/components/OnboardingButtons'
import PluginsSideBar from 'src/dataLoaders/components/collectorsWizard/configure/PluginsSideBar'

// Actions
import {
  setTelegrafConfigName,
  setTelegrafConfigDescription,
  setActiveTelegrafPlugin,
  setPluginConfiguration,
} from 'src/dataLoaders/actions/dataLoaders'
import {
  incrementCurrentStepIndex,
  decrementCurrentStepIndex,
} from 'src/dataLoaders/actions/steps'

// Types
import {AppState} from 'src/types/v2/index'
import {TelegrafPlugin} from 'src/types/v2/dataLoaders'

interface DispatchProps {
  onSetTelegrafConfigName: typeof setTelegrafConfigName
  onSetTelegrafConfigDescription: typeof setTelegrafConfigDescription
  onSetActiveTelegrafPlugin: typeof setActiveTelegrafPlugin
  onSetPluginConfiguration: typeof setPluginConfiguration
  onIncrementStep: typeof incrementCurrentStepIndex
  onDecrementStep: typeof decrementCurrentStepIndex
}

interface StateProps {
  telegrafConfigName: string
  telegrafConfigDescription: string
  telegrafPlugins: TelegrafPlugin[]
}

type Props = DispatchProps & StateProps

export class TelegrafPluginInstructions extends PureComponent<Props> {
  public render() {
    const {
      telegrafConfigName,
      telegrafConfigDescription,
      telegrafPlugins,
      onDecrementStep,
      onIncrementStep,
    } = this.props

    return (
      <Form onSubmit={onIncrementStep} className="data-loading--form">
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
              <FancyScrollbar
                autoHide={false}
                className="data-loading--scroll-content"
              >
                <Form.Element label="Telegraf Configuration Name">
                  <Input
                    type={InputType.Text}
                    value={telegrafConfigName}
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
                    onChange={this.handleDescriptionInput}
                    titleText="Telegraf Configuration Description"
                    size={ComponentSize.Medium}
                  />
                </Form.Element>
              </FancyScrollbar>
            </div>
          </div>
        </div>

        <OnboardingButtons
          onClickBack={onDecrementStep}
          nextButtonText={'Create and Verify'}
          className="data-loading--button-container"
        />
      </Form>
    )
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

const mstp = ({
  dataLoading: {
    dataLoaders: {
      telegrafConfigName,
      telegrafConfigDescription,
      telegrafPlugins,
    },
  },
}: AppState): StateProps => {
  return {
    telegrafConfigName,
    telegrafConfigDescription,
    telegrafPlugins,
  }
}

const mdtp: DispatchProps = {
  onSetTelegrafConfigName: setTelegrafConfigName,
  onSetTelegrafConfigDescription: setTelegrafConfigDescription,
  onIncrementStep: incrementCurrentStepIndex,
  onDecrementStep: decrementCurrentStepIndex,
  onSetActiveTelegrafPlugin: setActiveTelegrafPlugin,
  onSetPluginConfiguration: setPluginConfiguration,
}

export default connect<StateProps, DispatchProps, {}>(
  mstp,
  mdtp
)(TelegrafPluginInstructions)
