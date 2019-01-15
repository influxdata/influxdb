// Libraries
import React, {PureComponent} from 'react'
import {connect} from 'react-redux'
import _ from 'lodash'

// Components
import {ErrorHandling} from 'src/shared/decorators/errors'
import VerifyDataSwitcher from 'src/onboarding/components/verifyStep/VerifyDataSwitcher'
import OnboardingButtons from 'src/onboarding/components/OnboardingButtons'
import FancyScrollbar from 'src/shared/components/fancy_scrollbar/FancyScrollbar'

// Actions
import {
  setActiveTelegrafPlugin,
  createOrUpdateTelegrafConfigAsync,
  setPluginConfiguration,
} from 'src/onboarding/actions/dataLoaders'

// Types
import {DataLoaderType, TelegrafPlugin} from 'src/types/v2/dataLoaders'
import {Form} from 'src/clockface'
import {NotificationAction, RemoteDataState} from 'src/types'
import {StepStatus} from 'src/clockface/constants/wizard'
import {AppState} from 'src/types/v2'
import {DataLoaderStepProps} from 'src/dataLoaders/components/DataLoadersWizard'

export interface OwnProps extends DataLoaderStepProps {
  notify: NotificationAction
  type: DataLoaderType
  telegrafConfigID: string
  telegrafPlugins: TelegrafPlugin[]
  onSetActiveTelegrafPlugin: typeof setActiveTelegrafPlugin
  onSetPluginConfiguration: typeof setPluginConfiguration
  onSaveTelegrafConfig: typeof createOrUpdateTelegrafConfigAsync
  stepIndex: number
  bucket: string
  username: string
  org: string
}

interface StateProps {
  lpStatus: RemoteDataState
}

export type Props = OwnProps & StateProps

@ErrorHandling
export class VerifyDataStep extends PureComponent<Props> {
  public componentDidMount() {
    const {type, onSetPluginConfiguration, telegrafPlugins} = this.props

    if (type === DataLoaderType.Streaming) {
      telegrafPlugins.forEach(tp => {
        onSetPluginConfiguration(tp.name)
      })
    }
  }

  public render() {
    const {
      bucket,
      username,
      telegrafConfigID,
      type,
      onSaveTelegrafConfig,
      onDecrementCurrentStepIndex,
      onSetStepStatus,
      stepIndex,
      notify,
      lpStatus,
      org,
    } = this.props

    return (
      <div className="onboarding-step wizard--skippable">
        <Form onSubmit={this.handleIncrementStep}>
          <div className="wizard-step--scroll-area">
            <FancyScrollbar autoHide={false}>
              <div className="wizard-step--scroll-content">
                <VerifyDataSwitcher
                  notify={notify}
                  type={type}
                  telegrafConfigID={telegrafConfigID}
                  onSaveTelegrafConfig={onSaveTelegrafConfig}
                  org={org}
                  bucket={bucket}
                  username={username}
                  onSetStepStatus={onSetStepStatus}
                  stepIndex={stepIndex}
                  onDecrementCurrentStep={onDecrementCurrentStepIndex}
                  lpStatus={lpStatus}
                />
              </div>
            </FancyScrollbar>
          </div>
          <OnboardingButtons
            onClickBack={this.handleDecrementStep}
            onClickSkip={this.jumpToCompletionStep}
            skipButtonText={'Skip'}
            showSkip={true}
            nextButtonText={'Finish'}
          />
        </Form>
      </div>
    )
  }

  private get previousStepName() {
    const {telegrafPlugins, type} = this.props

    if (type === DataLoaderType.Streaming) {
      return _.get(telegrafPlugins, `${telegrafPlugins.length - 1}.name`, '')
    }

    return type
  }

  private handleIncrementStep = () => {
    const {onSetStepStatus, type, lpStatus, onExit} = this.props
    const {currentStepIndex} = this.props

    if (
      type === DataLoaderType.LineProtocol &&
      lpStatus === RemoteDataState.Error
    ) {
      onSetStepStatus(currentStepIndex, StepStatus.Error)
    } else {
      onSetStepStatus(currentStepIndex, StepStatus.Complete)
    }

    onExit()
  }

  private handleDecrementStep = () => {
    const {
      telegrafPlugins,
      onSetActiveTelegrafPlugin,
      onDecrementCurrentStepIndex,
      onSetSubstepIndex,
      stepIndex,
      type,
    } = this.props

    if (type === DataLoaderType.Streaming) {
      onSetSubstepIndex(stepIndex - 1, telegrafPlugins.length - 1 || 0)
      onSetActiveTelegrafPlugin(this.previousStepName)
    } else {
      onDecrementCurrentStepIndex()
      onSetActiveTelegrafPlugin('')
    }
  }

  private jumpToCompletionStep = () => {
    const {onSetCurrentStepIndex, stepStatuses} = this.props

    onSetCurrentStepIndex(stepStatuses.length - 1)
  }
}

const mstp = ({
  dataLoading: {
    dataLoaders: {lpStatus},
  },
}: AppState): StateProps => ({
  lpStatus,
})

export default connect<StateProps, {}, OwnProps>(mstp)(VerifyDataStep)
