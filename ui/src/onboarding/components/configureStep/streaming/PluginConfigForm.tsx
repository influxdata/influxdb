// Libraries
import React, {PureComponent} from 'react'
import _ from 'lodash'

// Components
import {Form} from 'src/clockface'
import ConfigFieldHandler from 'src/onboarding/components/configureStep/streaming/ConfigFieldHandler'

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
      <>
        <h3 className="wizard-step--title">{_.startCase(name)}</h3>
        <Form onSubmit={this.props.onClickNext}>
          <ConfigFieldHandler
            configFields={configFields}
            telegrafPlugin={telegrafPlugin}
            onSetConfigArrayValue={onSetConfigArrayValue}
            onAddConfigValue={onAddConfigValue}
            onRemoveConfigValue={onRemoveConfigValue}
            onUpdateTelegrafPluginConfig={onUpdateTelegrafPluginConfig}
          />
          <OnboardingButtons
            onClickBack={onClickPrevious}
            onClickSkip={onClickSkip}
            backButtonText={this.backButtonText}
            nextButtonText={this.nextButtonText}
            showSkip={true}
            skipButtonText={'Skip to Verify'}
            autoFocusNext={this.autoFocus}
          />
        </Form>
      </>
    )
  }
  private get autoFocus(): boolean {
    const {configFields} = this.props
    return !configFields
  }

  private get nextButtonText(): string {
    const {telegrafPlugins, currentIndex} = this.props

    if (currentIndex + 1 > telegrafPlugins.length - 1) {
      return 'Continue to Verify'
    }
    return `Continue to ${_.startCase(
      _.get(telegrafPlugins, `${currentIndex + 1}.name`)
    )}`
  }

  private get backButtonText(): string {
    const {telegrafPlugins, currentIndex} = this.props

    if (currentIndex < 1) {
      return 'Back to Select Streaming Sources'
    }
    return `Back to ${_.startCase(
      _.get(telegrafPlugins, `${currentIndex - 1}.name`)
    )}`
  }
}

export default PluginConfigForm
