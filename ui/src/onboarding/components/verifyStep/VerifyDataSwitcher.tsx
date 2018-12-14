// Libraries
import React, {PureComponent} from 'react'

// Components
import {ErrorHandling} from 'src/shared/decorators/errors'
import DataStreaming from 'src/onboarding/components/verifyStep/DataStreaming'

// Constants
import {StepStatus} from 'src/clockface/constants/wizard'

// Types
import {DataLoaderType} from 'src/types/v2/dataLoaders'

export interface Props {
  type: DataLoaderType
  org: string
  username: string
  bucket: string
  stepIndex: number
  onSetStepStatus: (index: number, status: StepStatus) => void
}

@ErrorHandling
class VerifyDataSwitcher extends PureComponent<Props> {
  public render() {
    const {org, username, bucket, type, stepIndex, onSetStepStatus} = this.props

    switch (type) {
      case DataLoaderType.Streaming:
        return (
          <DataStreaming
            org={org}
            username={username}
            bucket={bucket}
            onSetStepStatus={onSetStepStatus}
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
