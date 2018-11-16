// Libraries
import React, {PureComponent} from 'react'
import _ from 'lodash'

// Components
import InitStep from 'src/onboarding/components/InitStep'
import AdminStep from 'src/onboarding/components/AdminStep'
import SelectDataSourceStep from 'src/onboarding/components/SelectDataSourceStep'
import ConfigureDataSourceSwitcher from 'src/onboarding/components/ConfigureDataSourceSwitcher'
import CompletionStep from 'src/onboarding/components/CompletionStep'
import {ErrorHandling} from 'src/shared/decorators/errors'

// Actions
import {
  addDataSource,
  removeDataSource,
  setDataSources,
} from 'src/onboarding/actions/dataSources'

// Types
import {SetupParams} from 'src/onboarding/apis'
import {DataSource} from 'src/types/v2/dataSources'
import {OnboardingStepProps} from 'src/onboarding/containers/OnboardingWizard'

interface Props {
  onboardingStepProps: OnboardingStepProps
  onAddDataSource: typeof addDataSource
  onRemoveDataSource: typeof removeDataSource
  onSetDataSources: typeof setDataSources
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
      onSetDataSources,
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
            onSetDataSources={onSetDataSources}
          />
        )
      case 'Configure Data Sources':
        return (
          <ConfigureDataSourceSwitcher
            {...onboardingStepProps}
            dataSource={_.get(dataSources, '0.name', '')}
          />
        )
      case 'Complete':
        return <CompletionStep {...onboardingStepProps} />
      default:
        return <div />
    }
  }
}

export default OnboardingStepSwitcher
