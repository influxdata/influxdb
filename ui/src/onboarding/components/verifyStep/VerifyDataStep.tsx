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
  setPluginConfiguration,
} from 'src/dataLoaders/actions/dataLoaders'

// Types
import {DataLoaderType, TelegrafPlugin} from 'src/types/v2/dataLoaders'
import {Form} from 'src/clockface'
import {NotificationAction, RemoteDataState} from 'src/types'
import {AppState} from 'src/types/v2'
import {DataLoaderStepProps} from 'src/dataLoaders/components/DataLoadersWizard'

export interface OwnProps extends DataLoaderStepProps {
  notify: NotificationAction
  type: DataLoaderType
  telegrafConfigID: string
  telegrafPlugins: TelegrafPlugin[]
  onSetActiveTelegrafPlugin: typeof setActiveTelegrafPlugin
  onSetPluginConfiguration: typeof setPluginConfiguration
  stepIndex: number
  bucket: string
  username: string
  org: string
  selectedBucket: string
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
      username,
      telegrafConfigID,
      type,
      onDecrementCurrentStepIndex,
      notify,
      lpStatus,
      org,
    } = this.props

    return (
      <div className="onboarding-step">
        <Form onSubmit={this.handleIncrementStep}>
          <div className="wizard-step--scroll-area">
            <FancyScrollbar autoHide={false}>
              <div className="wizard-step--scroll-content">
                <VerifyDataSwitcher
                  notify={notify}
                  type={type}
                  telegrafConfigID={telegrafConfigID}
                  org={org}
                  bucket={this.bucket}
                  username={username}
                  onDecrementCurrentStep={onDecrementCurrentStepIndex}
                  lpStatus={lpStatus}
                />
              </div>
            </FancyScrollbar>
          </div>
          <OnboardingButtons
            onClickBack={this.handleDecrementStep}
            nextButtonText={'Finish'}
          />
        </Form>
      </div>
    )
  }

  private get bucket(): string {
    const {bucket, selectedBucket} = this.props

    return selectedBucket || bucket
  }

  private handleIncrementStep = () => {
    const {onExit} = this.props
    onExit()
  }

  private handleDecrementStep = () => {
    const {
      onSetActiveTelegrafPlugin,
      onDecrementCurrentStepIndex,
      onSetSubstepIndex,
      stepIndex,
      type,
    } = this.props

    if (type === DataLoaderType.Streaming) {
      onSetSubstepIndex(stepIndex - 1, 'config')
    } else {
      onDecrementCurrentStepIndex()
    }
    onSetActiveTelegrafPlugin('')
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
