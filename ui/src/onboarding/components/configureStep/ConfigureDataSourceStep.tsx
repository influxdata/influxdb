// Libraries
import React, {PureComponent} from 'react'
import _ from 'lodash'

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
import {DataLoaderStepProps} from 'src/dataLoaders/components/DataLoadersWizard'
import {
  TelegrafPlugin,
  DataLoaderType,
  ConfigurationState,
} from 'src/types/v2/dataLoaders'

export interface OwnProps extends DataLoaderStepProps {
  telegrafPlugins: TelegrafPlugin[]
  onSetActiveTelegrafPlugin: typeof setActiveTelegrafPlugin
  onUpdateTelegrafPluginConfig: typeof updateTelegrafPluginConfig
  onSetPluginConfiguration: typeof setPluginConfiguration
  type: DataLoaderType
  onAddConfigValue: typeof addConfigValue
  onRemoveConfigValue: typeof removeConfigValue
  onSetConfigArrayValue: typeof setConfigArrayValue
  bucket: string
  org: string
  username: string
}

type Props = OwnProps

@ErrorHandling
export class ConfigureDataSourceStep extends PureComponent<Props> {
  constructor(props: Props) {
    super(props)
  }

  public render() {
    const {
      telegrafPlugins,
      type,
      substep,
      onUpdateTelegrafPluginConfig,
      onAddConfigValue,
      onRemoveConfigValue,
      onSetConfigArrayValue,
      bucket,
      org,
      username,
    } = this.props

    return (
      <div className="onboarding-step wizard--skippable">
        <ConfigureDataSourceSwitcher
          bucket={bucket}
          org={org}
          username={username}
          telegrafPlugins={telegrafPlugins}
          onUpdateTelegrafPluginConfig={onUpdateTelegrafPluginConfig}
          onAddConfigValue={onAddConfigValue}
          onRemoveConfigValue={onRemoveConfigValue}
          dataLoaderType={type}
          currentIndex={+substep}
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
      substep,
      currentStepIndex,
      onSetSubstepIndex,
      type,
    } = this.props

    const index = +substep
    const telegrafPlugin = _.get(telegrafPlugins, `${index}.name`)

    onSetPluginConfiguration(telegrafPlugin)
    this.handleSetStepStatus()

    if (type === DataLoaderType.Streaming) {
      if (index >= telegrafPlugins.length - 1) {
        onIncrementCurrentStepIndex()
        onSetActiveTelegrafPlugin('')
      } else {
        const name = _.get(telegrafPlugins, `${index + 1}.name`, '')
        onSetActiveTelegrafPlugin(name)
        onSetSubstepIndex(+currentStepIndex, index + 1)
      }
      return
    }

    onIncrementCurrentStepIndex()
  }

  private handlePrevious = () => {
    const {
      type,
      substep,
      currentStepIndex,
      onSetActiveTelegrafPlugin,
      onSetPluginConfiguration,
      telegrafPlugins,
      onSetSubstepIndex,
      onDecrementCurrentStepIndex,
    } = this.props

    const index = +substep
    const telegrafPlugin = _.get(telegrafPlugins, `${index}.name`)

    if (type === DataLoaderType.Streaming) {
      onSetPluginConfiguration(telegrafPlugin)
      this.handleSetStepStatus()

      if (index > 0) {
        const name = _.get(telegrafPlugins, `${index - 1}.name`)
        onSetActiveTelegrafPlugin(name)
        onSetSubstepIndex(+currentStepIndex, index - 1)
      } else {
        onSetActiveTelegrafPlugin('')
        onSetSubstepIndex(+currentStepIndex - 1, 'streaming')
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
      currentStepIndex,
    } = this.props

    if (type === DataLoaderType.Streaming) {
      const unconfigured = telegrafPlugins.find(tp => {
        return tp.configured === ConfigurationState.Unconfigured
      })

      if (unconfigured || !telegrafPlugins.length) {
        onSetStepStatus(currentStepIndex, StepStatus.Incomplete)
      } else {
        onSetStepStatus(currentStepIndex, StepStatus.Complete)
      }
    } else {
      onSetStepStatus(currentStepIndex, StepStatus.Complete)
    }
  }
}

export default ConfigureDataSourceStep
