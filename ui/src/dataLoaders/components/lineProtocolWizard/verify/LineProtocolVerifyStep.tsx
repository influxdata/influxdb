// Libraries
import React, {PureComponent} from 'react'
import _ from 'lodash'

// Components
import {Form, Overlay} from '@influxdata/clockface'
import StatusIndicator from 'src/dataLoaders/components/lineProtocolWizard/verify/StatusIndicator'
import OnboardingButtons from 'src/onboarding/components/OnboardingButtons'

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
          <p>
            Need help writing InfluxDB Line Protocol?{' '}
            <a
              href="https://v2.docs.influxdata.com/v2.0/write-data/#write-data-in-the-influxdb-ui"
              target="_blank"
            >
              See Documentation
            </a>
          </p>
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
