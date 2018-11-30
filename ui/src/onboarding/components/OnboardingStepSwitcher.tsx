// Libraries
import React, {PureComponent} from 'react'
import _ from 'lodash'

// Components
import InitStep from 'src/onboarding/components/InitStep'
import AdminStep from 'src/onboarding/components/AdminStep'
import SelectDataSourceStep from 'src/onboarding/components/SelectDataSourceStep'
import ConfigureDataSourceStep from 'src/onboarding/components/ConfigureDataSourceStep'
import CompletionStep from 'src/onboarding/components/CompletionStep'
import {ErrorHandling} from 'src/shared/decorators/errors'

// Actions
import {
  addDataSource,
  removeDataSource,
  setDataLoadersType,
} from 'src/onboarding/actions/dataLoaders'

// Types
import {SetupParams} from 'src/onboarding/apis'
import {DataSource, DataSourceType} from 'src/types/v2/dataSources'
import {OnboardingStepProps} from 'src/onboarding/containers/OnboardingWizard'

interface Props {
  onboardingStepProps: OnboardingStepProps
  onAddDataSource: typeof addDataSource
  onRemoveDataSource: typeof removeDataSource
  onSetDataLoadersType: typeof setDataLoadersType
  setupParams: SetupParams
  dataLoaders: {dataSources: DataSource[]; type: DataSourceType}
  stepTitle: string
}

@ErrorHandling
class OnboardingStepSwitcher extends PureComponent<Props> {
  public render() {
    const {
      onboardingStepProps,
      stepTitle,
      setupParams,
      dataLoaders,
      onSetDataLoadersType,
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
            {...dataLoaders}
            onSetDataLoadersType={onSetDataLoadersType}
            bucket={_.get(setupParams, 'bucket', '')}
            onAddDataSource={onAddDataSource}
            onRemoveDataSource={onRemoveDataSource}
          />
        )
      case 'Configure Data Sources':
        return (
          <ConfigureDataSourceStep {...onboardingStepProps} {...dataLoaders} />
        )
      case 'Complete':
        return <CompletionStep {...onboardingStepProps} />
      default:
        return <div />
    }
  }
}

export default OnboardingStepSwitcher
