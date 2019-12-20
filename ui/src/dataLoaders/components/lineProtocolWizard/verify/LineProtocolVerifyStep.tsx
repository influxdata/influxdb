// Libraries
import React, {PureComponent} from 'react'
import _ from 'lodash'

// Components
import {Form, Overlay} from '@influxdata/clockface'
import StatusIndicator from 'src/dataLoaders/components/lineProtocolWizard/verify/StatusIndicator'
import OnboardingButtons from 'src/onboarding/components/OnboardingButtons'
import LineProtocolHelperText from 'src/dataLoaders/components/lineProtocolWizard/LineProtocolHelperText'

// Types
import {LineProtocolStepProps} from 'src/dataLoaders/components/lineProtocolWizard/LineProtocolWizard'

// Decorators
import {ErrorHandling} from 'src/shared/decorators/errors'

type Props = LineProtocolStepProps

@ErrorHandling
export class VerifyLineProtocolStep extends PureComponent<Props> {
  public render() {
    const {onDecrementCurrentStepIndex, onExit} = this.props

    return (
      <Form onSubmit={onExit}>
        <Overlay.Body style={{textAlign: 'center'}}>
          <StatusIndicator />
          <LineProtocolHelperText />
        </Overlay.Body>
        <OnboardingButtons
          onClickBack={onDecrementCurrentStepIndex}
          nextButtonText="Finish"
        />
      </Form>
    )
  }
}

export default VerifyLineProtocolStep
