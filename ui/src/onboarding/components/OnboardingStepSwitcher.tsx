// Libraries
import React, {PureComponent} from 'react'
import _ from 'lodash'

// Components
import InitStep from 'src/onboarding/components/InitStep'
import AdminStep from 'src/onboarding/components/AdminStep'
import SelectDataSourceStep from 'src/onboarding/components/SelectDataSourceStep'
import OtherStep from 'src/onboarding/components/OtherStep'
import CompletionStep from 'src/onboarding/components/CompletionStep'
import {ErrorHandling} from 'src/shared/decorators/errors'

// Actions
import {addDataSource, removeDataSource} from 'src/onboarding/actions'

// Types
import {SetupParams} from 'src/onboarding/apis'
import {DataSource} from 'src/types/v2/dataSources'
import {OnboardingStepProps} from 'src/onboarding/containers/OnboardingWizard'

interface Props {
  onboardingStepProps: OnboardingStepProps
  onAddDataSource: typeof addDataSource
  onRemoveDataSource: typeof removeDataSource
  setupParams: SetupParams
  dataSources: DataSource[]
  stepTitle: string
}

@ErrorHandling
class OnboardingStepSwitcher extends PureComponent<Props> {
  public render() {
    const {
      onboardingStepProps,
      stepTitle,
      setupParams,
      dataSources,
      onAddDataSource,
      onRemoveDataSource,
    } = this.props

    switch (stepTitle) {
      case 'Welcome':
        return <InitStep {...onboardingStepProps} />
      case 'Admin Setup':
        return <AdminStep {...onboardingStepProps} />
      case 'Select Data Sources':
        return (
          <SelectDataSourceStep
            {...onboardingStepProps}
            bucket={_.get(setupParams, 'bucket', '')}
            dataSources={dataSources}
            onAddDataSource={onAddDataSource}
            onRemoveDataSource={onRemoveDataSource}
          />
        )
      case 'Next Step':
        return <OtherStep {...onboardingStepProps} />
      case 'Complete':
        return <CompletionStep {...onboardingStepProps} />
    }
  }
}

export default OnboardingStepSwitcher
