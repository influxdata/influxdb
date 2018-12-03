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

// Types
import {OnboardingStepProps} from 'src/onboarding/containers/OnboardingWizard'
import {DataLoaderType} from 'src/types/v2/dataLoaders'

export interface Props extends OnboardingStepProps {
  type: DataLoaderType
}

@ErrorHandling
class VerifyDataStep extends PureComponent<Props> {
  public render() {
    const {
      setupParams,
      type,
      onDecrementCurrentStepIndex,
      onIncrementCurrentStepIndex,
    } = this.props

    return (
      <div className="onboarding-step">
        <VerifyDataSwitcher
          type={type}
          org={_.get(setupParams, 'org', '')}
          username={_.get(setupParams, 'username', '')}
          bucket={_.get(setupParams, 'bucket', '')}
        />
        <div className="wizard-button-bar">
          <Button
            color={ComponentColor.Default}
            text="Back"
            size={ComponentSize.Medium}
            onClick={onDecrementCurrentStepIndex}
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
      </div>
    )
  }
}

export default VerifyDataStep
