// Libraries
import React, {PureComponent} from 'react'
import _ from 'lodash'

// Components
import SelectCollectorsStep from 'src/dataLoaders/components/collectorsWizard/select/SelectCollectorsStep'
import PluginConfigSwitcher from 'src/dataLoaders/components/collectorsWizard/configure/PluginConfigSwitcher'
import VerifyCollectorsStep from 'src/dataLoaders/components/collectorsWizard/verify/VerifyCollectorsStep'
import {ErrorHandling} from 'src/shared/decorators/errors'

// Types
import {CollectorsStep} from 'src/types/v2/dataLoaders'
import {CollectorsStepProps} from 'src/dataLoaders/components/collectorsWizard/CollectorsWizard'
import {Bucket} from 'src/api'

interface Props {
  stepProps: CollectorsStepProps
  buckets: Bucket[]
}

@ErrorHandling
class StepSwitcher extends PureComponent<Props> {
  public render() {
    const {stepProps, buckets} = this.props

    switch (stepProps.currentStepIndex) {
      case CollectorsStep.Select:
        return <SelectCollectorsStep {...stepProps} buckets={buckets} />
      case CollectorsStep.Configure:
        return (
          <div className="onboarding-step wizard--skippable">
            <PluginConfigSwitcher />
          </div>
        )
      case CollectorsStep.Verify:
        return <VerifyCollectorsStep {...stepProps} />
      default:
        return <div />
    }
  }
}

export default StepSwitcher
