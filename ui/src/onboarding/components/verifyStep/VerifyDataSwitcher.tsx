// Libraries
import React, {PureComponent} from 'react'

// Components
import {ErrorHandling} from 'src/shared/decorators/errors'
import DataStreaming from 'src/onboarding/components/verifyStep/DataStreaming'

// Actions
import {createOrUpdateTelegrafConfigAsync} from 'src/onboarding/actions/dataLoaders'

// Constants
import {StepStatus} from 'src/clockface/constants/wizard'

// Types
import {DataLoaderType} from 'src/types/v2/dataLoaders'
import {NotificationAction} from 'src/types'

export interface Props {
  notify: NotificationAction
  type: DataLoaderType
  org: string
  bucket: string
  stepIndex: number
  authToken: string
  telegrafConfigID: string
  onSaveTelegrafConfig: typeof createOrUpdateTelegrafConfigAsync
  onSetStepStatus: (index: number, status: StepStatus) => void
}

@ErrorHandling
class VerifyDataSwitcher extends PureComponent<Props> {
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
        return <div>Yay data has been loaded into {bucket}!</div>
      default:
        return <div />
    }
  }
}

export default VerifyDataSwitcher
