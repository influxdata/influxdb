// Libraries
import React, {PureComponent} from 'react'
import {connect} from 'react-redux'

// Components
import {ErrorHandling} from 'src/shared/decorators/errors'
import DataStreaming from 'src/onboarding/components/verifyStep/DataStreaming'

// Actions
import {createOrUpdateTelegrafConfigAsync} from 'src/onboarding/actions/dataLoaders'

// Constants
import {StepStatus} from 'src/clockface/constants/wizard'

// Types
import {DataLoaderType} from 'src/types/v2/dataLoaders'
import {NotificationAction, RemoteDataState} from 'src/types'
import StatusIndicator from 'src/onboarding/components/verifyStep/lineProtocol/StatusIndicator'
import {AppState} from 'src/types/v2'

interface OwnProps {
  notify: NotificationAction
  type: DataLoaderType
  org: string
  bucket: string
  stepIndex: number
  authToken: string
  telegrafConfigID: string
  onSaveTelegrafConfig: typeof createOrUpdateTelegrafConfigAsync
  onSetStepStatus: (index: number, status: StepStatus) => void
  onDecrementCurrentStep: () => void
}

interface StateProps {
  lpStatus: RemoteDataState
}

export type Props = OwnProps & StateProps

@ErrorHandling
export class VerifyDataSwitcher extends PureComponent<Props> {
  public render() {
    const {
      org,
      bucket,
      type,
      stepIndex,
      onSetStepStatus,
      authToken,
      telegrafConfigID,
      onSaveTelegrafConfig,
      notify,
      lpStatus,
    } = this.props

    switch (type) {
      case DataLoaderType.Streaming:
        return (
          <DataStreaming
            notify={notify}
            org={org}
            configID={telegrafConfigID}
            authToken={authToken}
            bucket={bucket}
            onSetStepStatus={onSetStepStatus}
            onSaveTelegrafConfig={onSaveTelegrafConfig}
            stepIndex={stepIndex}
          />
        )
      case DataLoaderType.LineProtocol:
        return (
          <StatusIndicator
            status={lpStatus}
            onClickRetry={this.handleClickRetry}
          />
        )
      default:
        return <div />
    }
  }

  private handleClickRetry = () => {
    this.props.onDecrementCurrentStep()
  }
}

const mstp = ({
  onboarding: {
    dataLoaders: {lpStatus},
  },
}: AppState): StateProps => ({
  lpStatus,
})

export default connect<StateProps, {}, OwnProps>(mstp)(VerifyDataSwitcher)
