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
import {setActiveTelegrafPlugin} from 'src/onboarding/actions/dataLoaders'

// Types
import {OnboardingStepProps} from 'src/onboarding/containers/OnboardingWizard'
import {DataLoaderType, TelegrafPlugin} from 'src/types/v2/dataLoaders'

export interface Props extends OnboardingStepProps {
  type: DataLoaderType
  telegrafPlugins: TelegrafPlugin[]
  onSetActiveTelegrafPlugin: typeof setActiveTelegrafPlugin
  stepIndex: number
}

@ErrorHandling
class VerifyDataStep extends PureComponent<Props> {
  public render() {
    const {
      setupParams,
      type,
      onIncrementCurrentStepIndex,
      onSetStepStatus,
      stepIndex,
    } = this.props

    return (
      <div className="onboarding-step">
        <VerifyDataSwitcher
          type={type}
          org={_.get(setupParams, 'org', '')}
          username={_.get(setupParams, 'username', '')}
          bucket={_.get(setupParams, 'bucket', '')}
          onSetStepStatus={onSetStepStatus}
          stepIndex={stepIndex}
        />
        <div className="wizard-button-container">
          <div className="wizard-button-bar">
            <Button
              color={ComponentColor.Default}
              text="Back"
              size={ComponentSize.Medium}
              onClick={this.handleDecrementStep}
            />
            <Button
              color={ComponentColor.Primary}
              text="Next"
              size={ComponentSize.Medium}
              onClick={onIncrementCurrentStepIndex}
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

  private handleDecrementStep = () => {
    const {
      telegrafPlugins,
      onSetActiveTelegrafPlugin,
      onDecrementCurrentStepIndex,
    } = this.props

    const name = _.get(telegrafPlugins, `${telegrafPlugins.length - 1}.name`)
    onSetActiveTelegrafPlugin(name)

    onDecrementCurrentStepIndex()
  }

  private jumpToCompletionStep = () => {
    const {onSetCurrentStepIndex, stepStatuses} = this.props

    onSetCurrentStepIndex(stepStatuses.length - 1)
  }
}

export default VerifyDataStep
