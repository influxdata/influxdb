// Libraries
import React, {PureComponent} from 'react'
import _ from 'lodash'
import {withRouter, WithRouterProps} from 'react-router'

// Components
import {ErrorHandling} from 'src/shared/decorators/errors'
import ConfigureDataSourceSwitcher from 'src/onboarding/components/configureStep/ConfigureDataSourceSwitcher'

// Actions
import {setActiveTelegrafPlugin} from 'src/onboarding/actions/dataLoaders'
import {
  updateTelegrafPluginConfig,
  setPluginConfiguration,
  addConfigValue,
  removeConfigValue,
  setConfigArrayValue,
} from 'src/onboarding/actions/dataLoaders'

// Constants
import {StepStatus} from 'src/clockface/constants/wizard'

// Types
import {OnboardingStepProps} from 'src/onboarding/containers/OnboardingWizard'
import {
  TelegrafPlugin,
  DataLoaderType,
  ConfigurationState,
} from 'src/types/v2/dataLoaders'

export interface OwnProps extends OnboardingStepProps {
  telegrafPlugins: TelegrafPlugin[]
  onSetActiveTelegrafPlugin: typeof setActiveTelegrafPlugin
  onUpdateTelegrafPluginConfig: typeof updateTelegrafPluginConfig
  onSetPluginConfiguration: typeof setPluginConfiguration
  type: DataLoaderType
  onAddConfigValue: typeof addConfigValue
  onRemoveConfigValue: typeof removeConfigValue
  authToken: string
  onSetConfigArrayValue: typeof setConfigArrayValue
}

interface RouterProps {
  params: {
    stepID: string
    substepID: string
  }
}

type Props = OwnProps & WithRouterProps & RouterProps

@ErrorHandling
export class ConfigureDataSourceStep extends PureComponent<Props> {
  constructor(props: Props) {
    super(props)
  }

  public componentDidMount() {
    const {
      router,
      params: {stepID, substepID},
    } = this.props

    if (substepID === undefined) {
      router.replace(`/onboarding/${stepID}/0`)
    }
  }

  public render() {
    const {
      telegrafPlugins,
      type,
      params: {substepID},
      setupParams,
      onUpdateTelegrafPluginConfig,
      onAddConfigValue,
      onRemoveConfigValue,
      onSetConfigArrayValue,
    } = this.props

    return (
      <div className="onboarding-step">
        <ConfigureDataSourceSwitcher
          bucket={_.get(setupParams, 'bucket', '')}
          org={_.get(setupParams, 'org', '')}
          username={_.get(setupParams, 'username', '')}
          telegrafPlugins={telegrafPlugins}
          onUpdateTelegrafPluginConfig={onUpdateTelegrafPluginConfig}
          onAddConfigValue={onAddConfigValue}
          onRemoveConfigValue={onRemoveConfigValue}
          dataLoaderType={type}
          currentIndex={+substepID}
          onSetConfigArrayValue={onSetConfigArrayValue}
          onClickNext={this.handleNext}
          onClickPrevious={this.handlePrevious}
          onClickSkip={this.jumpToCompletionStep}
        />
      </div>
    )
  }

  private jumpToCompletionStep = () => {
    const {onSetCurrentStepIndex, stepStatuses, type} = this.props

    this.handleSetStepStatus()

    if (type === DataLoaderType.Streaming) {
      onSetCurrentStepIndex(stepStatuses.length - 2)
    } else {
      onSetCurrentStepIndex(stepStatuses.length - 1)
    }
  }

  private handleNext = async () => {
    const {
      onIncrementCurrentStepIndex,
      onSetActiveTelegrafPlugin,
      onSetPluginConfiguration,
      telegrafPlugins,
      params: {substepID, stepID},
      onSetSubstepIndex,
    } = this.props

    const index = +substepID
    const telegrafPlugin = _.get(telegrafPlugins, `${index}.name`)

    onSetPluginConfiguration(telegrafPlugin)
    this.handleSetStepStatus()

    if (index >= telegrafPlugins.length - 1) {
      onIncrementCurrentStepIndex()
      onSetActiveTelegrafPlugin('')
    } else {
      const name = _.get(telegrafPlugins, `${index + 1}.name`, '')
      onSetActiveTelegrafPlugin(name)
      onSetSubstepIndex(+stepID, index + 1)
    }
  }

  private handlePrevious = () => {
    const {
      type,
      onSetActiveTelegrafPlugin,
      onSetPluginConfiguration,
      params: {substepID, stepID},
      telegrafPlugins,
      onSetSubstepIndex,
      onDecrementCurrentStepIndex,
    } = this.props

    const index = +substepID
    const telegrafPlugin = _.get(telegrafPlugins, `${index}.name`)

    if (type === DataLoaderType.Streaming) {
      onSetPluginConfiguration(telegrafPlugin)
      this.handleSetStepStatus()

      if (index > 0) {
        const name = _.get(telegrafPlugins, `${index - 1}.name`)
        onSetActiveTelegrafPlugin(name)
        onSetSubstepIndex(+stepID, index - 1)
      } else {
        onSetActiveTelegrafPlugin('')
        onSetSubstepIndex(+stepID - 1, 'streaming')
      }

      return
    }

    onDecrementCurrentStepIndex()
  }

  private handleSetStepStatus = () => {
    const {
      type,
      telegrafPlugins,
      onSetStepStatus,
      params: {stepID},
    } = this.props

    if (type === DataLoaderType.Streaming) {
      const unconfigured = telegrafPlugins.find(tp => {
        return tp.configured === ConfigurationState.Unconfigured
      })

      if (unconfigured || !telegrafPlugins.length) {
        onSetStepStatus(parseInt(stepID, 10), StepStatus.Incomplete)
      } else {
        onSetStepStatus(parseInt(stepID, 10), StepStatus.Complete)
      }
    } else {
      onSetStepStatus(parseInt(stepID, 10), StepStatus.Complete)
    }
  }
}

export default withRouter<OwnProps>(ConfigureDataSourceStep)
