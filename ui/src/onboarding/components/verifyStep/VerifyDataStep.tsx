// Libraries
import React, {PureComponent} from 'react'
import _ from 'lodash'

// Components
import {ErrorHandling} from 'src/shared/decorators/errors'
import {
  Button,
  ComponentColor,
  ComponentSize,
  ComponentStatus,
} from 'src/clockface'
import VerifyDataSwitcher from 'src/onboarding/components/verifyStep/VerifyDataSwitcher'

// Actions
import {
  setActiveTelegrafPlugin,
  createOrUpdateTelegrafConfigAsync,
} from 'src/onboarding/actions/dataLoaders'

// Types
import {OnboardingStepProps} from 'src/onboarding/containers/OnboardingWizard'
import {DataLoaderType, TelegrafPlugin} from 'src/types/v2/dataLoaders'

export interface Props extends OnboardingStepProps {
  type: DataLoaderType
  authToken: string
  telegrafConfigID: string
  telegrafPlugins: TelegrafPlugin[]
  onSetActiveTelegrafPlugin: typeof setActiveTelegrafPlugin
  onSaveTelegrafConfig: typeof createOrUpdateTelegrafConfigAsync
  stepIndex: number
}

@ErrorHandling
class VerifyDataStep extends PureComponent<Props> {
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
    } = this.props

    return (
      <div className="onboarding-step">
        <VerifyDataSwitcher
          type={type}
          telegrafConfigID={telegrafConfigID}
          authToken={authToken}
          onSaveTelegrafConfig={onSaveTelegrafConfig}
          org={_.get(setupParams, 'org', '')}
          bucket={_.get(setupParams, 'bucket', '')}
          onSetStepStatus={onSetStepStatus}
          stepIndex={stepIndex}
        />
        <div className="wizard--button-container">
          <div className="wizard--button-bar">
            <Button
              color={ComponentColor.Default}
              text={this.backButtonText}
              size={ComponentSize.Medium}
              onClick={this.handleDecrementStep}
              data-test="back"
            />
            <Button
              color={ComponentColor.Primary}
              text="Continue to Completion"
              size={ComponentSize.Medium}
              onClick={onIncrementCurrentStepIndex}
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

  private get skipLink() {
    return (
      <Button
        color={ComponentColor.Default}
        text="Skip"
        customClass="wizard--skip-button"
        size={ComponentSize.Medium}
        onClick={this.jumpToCompletionStep}
      >
        skip
      </Button>
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
