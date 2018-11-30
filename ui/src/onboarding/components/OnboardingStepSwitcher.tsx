// Libraries
import React, {PureComponent} from 'react'
import _ from 'lodash'

// Components
import InitStep from 'src/onboarding/components/InitStep'
import AdminStep from 'src/onboarding/components/AdminStep'
import SelectDataSourceStep from 'src/onboarding/components/selectionStep/SelectDataSourceStep'
import ConfigureDataSourceStep from 'src/onboarding/components/configureStep/ConfigureDataSourceStep'
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
import {DataSource, DataLoaderType} from 'src/types/v2/dataLoaders'
import {OnboardingStepProps} from 'src/onboarding/containers/OnboardingWizard'

interface Props {
  onboardingStepProps: OnboardingStepProps
  onAddDataSource: typeof addDataSource
  onRemoveDataSource: typeof removeDataSource
  onSetDataLoadersType: typeof setDataLoadersType
  setupParams: SetupParams
  dataLoaders: {dataSources: DataSource[]; type: DataLoaderType}
  currentStepIndex: number
}

@ErrorHandling
class OnboardingStepSwitcher extends PureComponent<Props> {
  public render() {
    const {
      currentStepIndex,
      onboardingStepProps,
      setupParams,
      dataLoaders,
      onSetDataLoadersType,
      onAddDataSource,
      onRemoveDataSource,
    } = this.props

    switch (currentStepIndex) {
      case 0:
        return <InitStep {...onboardingStepProps} />
      case 1:
        return <AdminStep {...onboardingStepProps} />
      case 2:
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
      case 3:
        return (
          <ConfigureDataSourceStep {...onboardingStepProps} {...dataLoaders} />
        )
      case 4:
        return <CompletionStep {...onboardingStepProps} />
      default:
        return <div />
    }
  }
}

export default OnboardingStepSwitcher
