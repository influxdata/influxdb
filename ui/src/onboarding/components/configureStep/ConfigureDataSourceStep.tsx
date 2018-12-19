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
  setConfigArrayValue,
} from 'src/onboarding/actions/dataLoaders'

// Constants
import {StepStatus} from 'src/clockface/constants/wizard'

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
  authToken: string
  onSetConfigArrayValue: typeof setConfigArrayValue
}

interface RouterProps {
  params: {
    stepID: string
    substepID: string
  }
}

type Props = OwnProps & WithRouterProps & RouterProps

@ErrorHandling
export class ConfigureDataSourceStep extends PureComponent<Props> {
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
      params: {substepID},
      setupParams,
      onUpdateTelegrafPluginConfig,
      onSetPluginConfiguration,
      onAddConfigValue,
      onRemoveConfigValue,
      onSetConfigArrayValue,
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
          onSetConfigArrayValue={onSetConfigArrayValue}
        />
        <div className="wizard--button-container">
          <div className="wizard--button-bar">
            <Button
              color={ComponentColor.Default}
              text={this.backButtonText}
              size={ComponentSize.Medium}
              onClick={this.handlePrevious}
              data-test="back"
            />
            <Button
              color={ComponentColor.Primary}
              text={this.nextButtonText}
              size={ComponentSize.Medium}
              onClick={this.handleNext}
              status={ComponentStatus.Default}
              titleText={'Next'}
              data-test="next"
            />
          </div>
          {this.skipLink}
        </div>
      </div>
    )
  }

  private get nextButtonText(): string {
    const {
      telegrafPlugins,
      params: {substepID},
      type,
    } = this.props

    const index = +substepID

    if (type === DataLoaderType.Streaming) {
      if (index + 1 > telegrafPlugins.length - 1) {
        return 'Continue to Verify'
      }
      return `Continue to ${_.startCase(
        _.get(telegrafPlugins, `${index + 1}.name`)
      )}`
    }

    return 'Continue to Verify'
  }

  private get backButtonText(): string {
    const {
      telegrafPlugins,
      params: {substepID},
      type,
    } = this.props

    const index = +substepID

    if (type === DataLoaderType.Streaming) {
      if (index < 1) {
        return 'Back to Select Streaming Sources'
      }
      return `Back to ${_.startCase(
        _.get(telegrafPlugins, `${index - 1}.name`)
      )}`
    }

    return 'Back to Select Data Source Type'
  }

  private get skipLink() {
    const {type} = this.props
    const skipText =
      type === DataLoaderType.Streaming ? 'Skip to Verify' : 'Skip Config'

    return (
      <Button
        customClass="wizard--skip-button"
        size={ComponentSize.Medium}
        color={ComponentColor.Default}
        text={skipText}
        onClick={this.jumpToCompletionStep}
        data-test="skip"
      />
    )
  }

  private jumpToCompletionStep = () => {
    const {onSetCurrentStepIndex, stepStatuses, type} = this.props

    this.handleSetStepStatus()

    if (type === DataLoaderType.Streaming) {
      onSetCurrentStepIndex(stepStatuses.length - 2)
    } else {
      onSetCurrentStepIndex(stepStatuses.length - 1)
    }
  }

  private handleNext = async () => {
    const {
      onIncrementCurrentStepIndex,
      onSetActiveTelegrafPlugin,
      onSetPluginConfiguration,
      telegrafPlugins,
      params: {substepID, stepID},
      onSetSubstepIndex,
    } = this.props

    const index = +substepID
    const telegrafPlugin = _.get(telegrafPlugins, `${index}.name`)

    onSetPluginConfiguration(telegrafPlugin)
    this.handleSetStepStatus()

    if (index >= telegrafPlugins.length - 1) {
      onIncrementCurrentStepIndex()
      onSetActiveTelegrafPlugin('')
    } else {
      const name = _.get(telegrafPlugins, `${index + 1}.name`, '')
      onSetActiveTelegrafPlugin(name)
      onSetSubstepIndex(+stepID, index + 1)
    }
  }

  private handlePrevious = () => {
    const {
      type,
      onSetActiveTelegrafPlugin,
      onSetPluginConfiguration,
      params: {substepID, stepID},
      telegrafPlugins,
      onSetSubstepIndex,
      onDecrementCurrentStepIndex,
    } = this.props

    const index = +substepID
    const telegrafPlugin = _.get(telegrafPlugins, `${index}.name`)

    if (type === DataLoaderType.Streaming) {
      onSetPluginConfiguration(telegrafPlugin)
      this.handleSetStepStatus()

      if (index > 0) {
        const name = _.get(telegrafPlugins, `${index - 1}.name`)
        onSetActiveTelegrafPlugin(name)
        onSetSubstepIndex(+stepID, index - 1)
      } else {
        onSetActiveTelegrafPlugin('')
        onSetSubstepIndex(+stepID - 1, 'streaming')
      }

      return
    }

    onDecrementCurrentStepIndex()
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
