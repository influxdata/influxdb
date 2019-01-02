// Libraries
import React, {PureComponent} from 'react'
import _ from 'lodash'

// Components
import {ErrorHandling} from 'src/shared/decorators/errors'
import VerifyDataSwitcher from 'src/onboarding/components/verifyStep/VerifyDataSwitcher'
import OnboardingButtons from 'src/onboarding/components/OnboardingButtons'

// Actions
import {
  setActiveTelegrafPlugin,
  createOrUpdateTelegrafConfigAsync,
  setPluginConfiguration,
} from 'src/onboarding/actions/dataLoaders'

// Types
import {OnboardingStepProps} from 'src/onboarding/containers/OnboardingWizard'
import {DataLoaderType, TelegrafPlugin} from 'src/types/v2/dataLoaders'
import {Form} from 'src/clockface'
import {NotificationAction} from 'src/types'

export interface Props extends OnboardingStepProps {
  notify: NotificationAction
  type: DataLoaderType
  authToken: string
  telegrafConfigID: string
  telegrafPlugins: TelegrafPlugin[]
  onSetActiveTelegrafPlugin: typeof setActiveTelegrafPlugin
  onSetPluginConfiguration: typeof setPluginConfiguration
  onSaveTelegrafConfig: typeof createOrUpdateTelegrafConfigAsync
  stepIndex: number
}

@ErrorHandling
class VerifyDataStep extends PureComponent<Props> {
  public componentDidMount() {
    const {type, onSetPluginConfiguration, telegrafPlugins} = this.props

    if (type === DataLoaderType.Streaming) {
      telegrafPlugins.forEach(tp => {
        onSetPluginConfiguration(tp.name)
      })
    }
  }

  public render() {
    const {
      setupParams,
      telegrafConfigID,
      authToken,
      type,
      onSaveTelegrafConfig,
      onIncrementCurrentStepIndex,
      onSetStepStatus,
      stepIndex,
      notify,
    } = this.props

    return (
      <Form onSubmit={onIncrementCurrentStepIndex}>
        <div className="onboarding-step">
          <VerifyDataSwitcher
            notify={notify}
            type={type}
            telegrafConfigID={telegrafConfigID}
            authToken={authToken}
            onSaveTelegrafConfig={onSaveTelegrafConfig}
            org={_.get(setupParams, 'org', '')}
            bucket={_.get(setupParams, 'bucket', '')}
            onSetStepStatus={onSetStepStatus}
            stepIndex={stepIndex}
          />
          <OnboardingButtons
            onClickBack={this.handleDecrementStep}
            onClickSkip={this.jumpToCompletionStep}
            nextButtonText={'Continue to Completion'}
            backButtonText={this.backButtonText}
            skipButtonText={'Skip'}
            showSkip={true}
          />
        </div>
      </Form>
    )
  }

  private get backButtonText(): string {
    return `Back to ${_.startCase(this.previousStepName) || ''} Configuration`
  }

  private get previousStepName() {
    const {telegrafPlugins, type} = this.props

    if (type === DataLoaderType.Streaming) {
      return _.get(telegrafPlugins, `${telegrafPlugins.length - 1}.name`, '')
    }

    return type
  }

  private handleDecrementStep = () => {
    const {
      telegrafPlugins,
      onSetActiveTelegrafPlugin,
      onDecrementCurrentStepIndex,
      onSetSubstepIndex,
      stepIndex,
      type,
    } = this.props

    if (type === DataLoaderType.Streaming) {
      onSetSubstepIndex(stepIndex - 1, telegrafPlugins.length - 1 || 0)
      onSetActiveTelegrafPlugin(this.previousStepName)
    } else {
      onDecrementCurrentStepIndex()
      onSetActiveTelegrafPlugin('')
    }
  }

  private jumpToCompletionStep = () => {
    const {onSetCurrentStepIndex, stepStatuses} = this.props

    onSetCurrentStepIndex(stepStatuses.length - 1)
  }
}

export default VerifyDataStep
