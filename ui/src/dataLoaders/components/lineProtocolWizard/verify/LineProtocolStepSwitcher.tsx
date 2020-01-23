// Libraries
import React, {PureComponent} from 'react'

// Components
import {ErrorHandling} from 'src/shared/decorators/errors'
import LineProtocol from 'src/dataLoaders/components/lineProtocolWizard/configure/LineProtocol'
import LineProtocolVerifyStep from 'src/dataLoaders/components/lineProtocolWizard/verify/LineProtocolVerifyStep'

// Types
import {LineProtocolStepProps} from 'src/dataLoaders/components/lineProtocolWizard/LineProtocolWizard'
import {Bucket, LineProtocolStep} from 'src/types'

interface Props {
  stepProps: LineProtocolStepProps
  buckets: Bucket[]
}

@ErrorHandling
class StepSwitcher extends PureComponent<Props> {
  public render() {
    const {stepProps} = this.props

    switch (stepProps.currentStepIndex) {
      case LineProtocolStep.Configure:
        return <LineProtocol {...stepProps} />
      case LineProtocolStep.Verify:
        return <LineProtocolVerifyStep {...stepProps} />
      default:
        return <div />
    }
  }
}

export default StepSwitcher
