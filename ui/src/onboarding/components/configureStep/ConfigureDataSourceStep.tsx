// Libraries
import React, {PureComponent} from 'react'
import _ from 'lodash'
import {withRouter, WithRouterProps} from 'react-router'

// Components
import {ErrorHandling} from 'src/shared/decorators/errors'
import {
  Button,
  ComponentColor,
  ComponentSize,
  ComponentStatus,
} from 'src/clockface'
import ConfigureDataSourceSwitcher from 'src/onboarding/components/configureStep/ConfigureDataSourceSwitcher'

// Actions
import {setActiveTelegrafPlugin} from 'src/onboarding/actions/dataLoaders'
import {
  updateTelegrafPluginConfig,
  setPluginConfiguration,
  addConfigValue,
  removeConfigValue,
  createTelegrafConfigAsync,
} from 'src/onboarding/actions/dataLoaders'

// Constants
import {StepStatus} from 'src/clockface/constants/wizard'
import {
  TelegrafConfigCreationSuccess,
  TelegrafConfigCreationError,
} from 'src/shared/copy/notifications'

// Types
import {OnboardingStepProps} from 'src/onboarding/containers/OnboardingWizard'
import {
  TelegrafPlugin,
  DataLoaderType,
  ConfigurationState,
} from 'src/types/v2/dataLoaders'

export interface OwnProps extends OnboardingStepProps {
  telegrafPlugins: TelegrafPlugin[]
  onSetActiveTelegrafPlugin: typeof setActiveTelegrafPlugin
  onUpdateTelegrafPluginConfig: typeof updateTelegrafPluginConfig
  onSetPluginConfiguration: typeof setPluginConfiguration
  type: DataLoaderType
  onAddConfigValue: typeof addConfigValue
  onRemoveConfigValue: typeof removeConfigValue
  onSaveTelegrafConfig: typeof createTelegrafConfigAsync
  authToken: string
}

interface RouterProps {
  params: {
    stepID: string
    substepID: string
  }
}

type Props = OwnProps & WithRouterProps & RouterProps

@ErrorHandling
class ConfigureDataSourceStep extends PureComponent<Props> {
  constructor(props: Props) {
    super(props)
  }

  public componentDidMount() {
    const {
      router,
      params: {stepID, substepID},
    } = this.props

    if (substepID === undefined) {
      router.replace(`/onboarding/${stepID}/0`)
    }
  }

  public render() {
    const {
      telegrafPlugins,
      type,
      authToken,
      params: {substepID},
      setupParams,
      onUpdateTelegrafPluginConfig,
      onSetPluginConfiguration,
      onAddConfigValue,
      onRemoveConfigValue,
    } = this.props

    return (
      <div className="onboarding-step">
        <ConfigureDataSourceSwitcher
          bucket={_.get(setupParams, 'bucket', '')}
          org={_.get(setupParams, 'org', '')}
          username={_.get(setupParams, 'username', '')}
          telegrafPlugins={telegrafPlugins}
          onUpdateTelegrafPluginConfig={onUpdateTelegrafPluginConfig}
          onSetPluginConfiguration={onSetPluginConfiguration}
          onAddConfigValue={onAddConfigValue}
          onRemoveConfigValue={onRemoveConfigValue}
          dataLoaderType={type}
          currentIndex={+substepID}
          authToken={authToken}
        />
        <div className="wizard-button-container">
          <div className="wizard-button-bar">
            <Button
              color={ComponentColor.Default}
              text="Back"
              size={ComponentSize.Medium}
              onClick={this.handlePrevious}
            />
            <Button
              color={ComponentColor.Primary}
              text="Next"
              size={ComponentSize.Medium}
              onClick={this.handleNext}
              status={ComponentStatus.Default}
              titleText={'Next'}
            />
          </div>
          {this.skipLink}
        </div>
      </div>
    )
  }

  private get skipLink() {
    return (
      <Button
        color={ComponentColor.Default}
        text="Skip"
        size={ComponentSize.Small}
        onClick={this.jumpToCompletionStep}
      >
        skip
      </Button>
    )
  }

  private jumpToCompletionStep = () => {
    const {onSetCurrentStepIndex, stepStatuses} = this.props

    this.handleSetStepStatus()
    onSetCurrentStepIndex(stepStatuses.length - 1)
  }

  private handleNext = async () => {
    const {
      onIncrementCurrentStepIndex,
      onSetActiveTelegrafPlugin,
      onSetPluginConfiguration,
      telegrafPlugins,
      authToken,
      notify,
      params: {substepID, stepID},
      router,
      type,
      onSaveTelegrafConfig,
    } = this.props

    const index = +substepID
    const telegrafPlugin = _.get(telegrafPlugins, `${index}.name`)

    onSetPluginConfiguration(telegrafPlugin)
    this.handleSetStepStatus()

    if (index >= telegrafPlugins.length - 1) {
      if (type === DataLoaderType.Streaming) {
        try {
          await onSaveTelegrafConfig(authToken)
          notify(TelegrafConfigCreationSuccess)
        } catch (error) {
          notify(TelegrafConfigCreationError)
        }
      }

      onIncrementCurrentStepIndex()
      onSetActiveTelegrafPlugin('')
    } else {
      const name = _.get(telegrafPlugins, `${index + 1}.name`, '')
      onSetActiveTelegrafPlugin(name)

      router.push(`/onboarding/${stepID}/${index + 1}`)
    }
  }

  private handlePrevious = () => {
    const {
      router,
      type,
      onSetActiveTelegrafPlugin,
      onSetPluginConfiguration,
      params: {substepID},
      telegrafPlugins,
    } = this.props

    const index = +substepID
    const telegrafPlugin = _.get(telegrafPlugins, `${index}.name`)

    if (type === DataLoaderType.Streaming) {
      onSetPluginConfiguration(telegrafPlugin)
      this.handleSetStepStatus()
    }

    if (index >= 0) {
      const name = _.get(telegrafPlugins, `${index - 1}.name`)
      onSetActiveTelegrafPlugin(name)
    } else {
      onSetActiveTelegrafPlugin('')
    }

    router.goBack()
  }

  private handleSetStepStatus = () => {
    const {
      type,
      telegrafPlugins,
      onSetStepStatus,
      params: {stepID},
    } = this.props

    if (type === DataLoaderType.Streaming) {
      const unconfigured = telegrafPlugins.find(tp => {
        return tp.configured === ConfigurationState.Unconfigured
      })

      if (unconfigured || !telegrafPlugins.length) {
        onSetStepStatus(parseInt(stepID, 10), StepStatus.Incomplete)
      } else {
        onSetStepStatus(parseInt(stepID, 10), StepStatus.Complete)
      }
    }
  }
}

export default withRouter<OwnProps>(ConfigureDataSourceStep)
