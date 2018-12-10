// Libraries
import React, {PureComponent} from 'react'
import _ from 'lodash'

// Components
import InitStep from 'src/onboarding/components/InitStep'
import AdminStep from 'src/onboarding/components/AdminStep'
import SelectDataSourceStep from 'src/onboarding/components/selectionStep/SelectDataSourceStep'
import ConfigureDataSourceStep from 'src/onboarding/components/configureStep/ConfigureDataSourceStep'
import CompletionStep from 'src/onboarding/components/CompletionStep'
import VerifyDataStep from 'src/onboarding/components/verifyStep/VerifyDataStep'
import {ErrorHandling} from 'src/shared/decorators/errors'

// Actions
import {
  addTelegrafPlugin,
  removeTelegrafPlugin,
  setDataLoadersType,
  setActiveTelegrafPlugin,
} from 'src/onboarding/actions/dataLoaders'

// Types
import {SetupParams} from 'src/onboarding/apis'
import {TelegrafPlugin, DataLoaderType} from 'src/types/v2/dataLoaders'
import {OnboardingStepProps} from 'src/onboarding/containers/OnboardingWizard'

interface Props {
  onboardingStepProps: OnboardingStepProps
  onAddTelegrafPlugin: typeof addTelegrafPlugin
  onRemoveTelegrafPlugin: typeof removeTelegrafPlugin
  onSetDataLoadersType: typeof setDataLoadersType
  onSetActiveTelegrafPlugin: typeof setActiveTelegrafPlugin
  setupParams: SetupParams
  dataLoaders: {telegrafPlugins: TelegrafPlugin[]; type: DataLoaderType}
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
      onAddTelegrafPlugin,
      onRemoveTelegrafPlugin,
      onSetActiveTelegrafPlugin,
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
            onAddTelegrafPlugin={onAddTelegrafPlugin}
            onRemoveTelegrafPlugin={onRemoveTelegrafPlugin}
            onSetActiveTelegrafPlugin={onSetActiveTelegrafPlugin}
          />
        )
      case 3:
        return (
          <ConfigureDataSourceStep
            {...onboardingStepProps}
            {...dataLoaders}
            onSetActiveTelegrafPlugin={onSetActiveTelegrafPlugin}
          />
        )
      case 4:
        return (
          <VerifyDataStep
            {...onboardingStepProps}
            {...dataLoaders}
            onSetActiveTelegrafPlugin={onSetActiveTelegrafPlugin}
          />
        )
      case 5:
        return <CompletionStep {...onboardingStepProps} />
      default:
        return <div />
    }
  }
}

export default OnboardingStepSwitcher
