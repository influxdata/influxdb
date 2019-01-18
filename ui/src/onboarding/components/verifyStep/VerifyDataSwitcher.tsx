// Libraries
import React, {PureComponent} from 'react'

// Components
import {ErrorHandling} from 'src/shared/decorators/errors'
import DataStreaming from 'src/onboarding/components/verifyStep/DataStreaming'
import FetchAuthToken from 'src/onboarding/components/verifyStep/FetchAuthToken'

// Actions
import {createOrUpdateTelegrafConfigAsync} from 'src/onboarding/actions/dataLoaders'

// Types
import {DataLoaderType} from 'src/types/v2/dataLoaders'
import {NotificationAction, RemoteDataState} from 'src/types'
import StatusIndicator from 'src/onboarding/components/verifyStep/lineProtocol/StatusIndicator'

interface Props {
  notify: NotificationAction
  type: DataLoaderType
  org: string
  bucket: string
  username: string
  telegrafConfigID: string
  onSaveTelegrafConfig: typeof createOrUpdateTelegrafConfigAsync
  onDecrementCurrentStep: () => void
  lpStatus: RemoteDataState
}

@ErrorHandling
export class VerifyDataSwitcher extends PureComponent<Props> {
  public render() {
    const {
      org,
      bucket,
      username,
      type,
      telegrafConfigID,
      onSaveTelegrafConfig,
      notify,
      lpStatus,
    } = this.props

    switch (type) {
      case DataLoaderType.Streaming:
        return (
          <FetchAuthToken bucket={bucket} username={username}>
            {authToken => (
              <DataStreaming
                notify={notify}
                org={org}
                configID={telegrafConfigID}
                authToken={authToken}
                bucket={bucket}
                onSaveTelegrafConfig={onSaveTelegrafConfig}
              />
            )}
          </FetchAuthToken>
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

export default VerifyDataSwitcher
