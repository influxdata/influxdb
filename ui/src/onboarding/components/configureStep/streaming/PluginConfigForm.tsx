// Libraries
import React, {PureComponent} from 'react'
import _ from 'lodash'

// Components
import {Form} from 'src/clockface'
import ConfigFieldHandler from 'src/onboarding/components/configureStep/streaming/ConfigFieldHandler'
import FancyScrollbar from 'src/shared/components/fancy_scrollbar/FancyScrollbar'

// Actions
import {
  updateTelegrafPluginConfig,
  addConfigValue,
  removeConfigValue,
  setConfigArrayValue,
} from 'src/onboarding/actions/dataLoaders'

// Types
import {TelegrafPlugin, ConfigFields} from 'src/types/v2/dataLoaders'
import OnboardingButtons from 'src/onboarding/components/OnboardingButtons'

interface Props {
  telegrafPlugin: TelegrafPlugin
  configFields: ConfigFields
  onUpdateTelegrafPluginConfig: typeof updateTelegrafPluginConfig
  onAddConfigValue: typeof addConfigValue
  onRemoveConfigValue: typeof removeConfigValue
  onSetConfigArrayValue: typeof setConfigArrayValue
  onClickNext: () => void
  telegrafPlugins: TelegrafPlugin[]
  currentIndex: number
  onClickPrevious: () => void
  onClickSkip: () => void
}

class PluginConfigForm extends PureComponent<Props> {
  public render() {
    const {
      telegrafPlugin: {name},
      configFields,
      telegrafPlugin,
      onSetConfigArrayValue,
      onAddConfigValue,
      onRemoveConfigValue,
      onUpdateTelegrafPluginConfig,
      onClickPrevious,
      onClickSkip,
    } = this.props
    return (
      <Form onSubmit={this.props.onClickNext}>
        <div className="wizard-step--scroll-area">
          <FancyScrollbar autoHide={false}>
            <div className="wizard-step--scroll-content">
              <h3 className="wizard-step--title">{_.startCase(name)}</h3>
              <h5 className="wizard-step--sub-title">
                For more information about this plugin, see{' '}
                <a
                  target="_blank"
                  href={`https://github.com/influxdata/telegraf/tree/master/plugins/inputs/${name}`}
                >
                  Documentation
                </a>
              </h5>
              <ConfigFieldHandler
                configFields={configFields}
                telegrafPlugin={telegrafPlugin}
                onSetConfigArrayValue={onSetConfigArrayValue}
                onAddConfigValue={onAddConfigValue}
                onRemoveConfigValue={onRemoveConfigValue}
                onUpdateTelegrafPluginConfig={onUpdateTelegrafPluginConfig}
              />
            </div>
          </FancyScrollbar>
        </div>
        <OnboardingButtons
          onClickBack={onClickPrevious}
          onClickSkip={onClickSkip}
          showSkip={true}
          skipButtonText={'Skip to Verify'}
          autoFocusNext={this.autoFocus}
        />
      </Form>
    )
  }
  private get autoFocus(): boolean {
    const {configFields} = this.props
    return !configFields
  }
}

export default PluginConfigForm
