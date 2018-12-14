// Libraries
import React, {PureComponent} from 'react'
import _ from 'lodash'

// Components
import TelegrafInstructions from 'src/onboarding/components/verifyStep/TelegrafInstructions'
import FetchConfigID from 'src/onboarding/components/verifyStep/FetchConfigID'
import FetchAuthToken from 'src/onboarding/components/verifyStep/FetchAuthToken'
import DataListening from 'src/onboarding/components/verifyStep/DataListening'

// Constants
import {StepStatus} from 'src/clockface/constants/wizard'

// Decorator
import {ErrorHandling} from 'src/shared/decorators/errors'

interface Props {
  bucket: string
  org: string
  username: string
  stepIndex: number
  onSetStepStatus: (index: number, status: StepStatus) => void
}

@ErrorHandling
class DataStreaming extends PureComponent<Props> {
  public render() {
    return (
      <>
        <FetchConfigID org={this.props.org}>
          {configID => (
            <FetchAuthToken
              bucket={this.props.bucket}
              username={this.props.username}
            >
              {authToken => (
                <TelegrafInstructions
                  authToken={authToken}
                  configID={configID}
                />
              )}
            </FetchAuthToken>
          )}
        </FetchConfigID>

        <DataListening
          bucket={this.props.bucket}
          stepIndex={this.props.stepIndex}
          onSetStepStatus={this.props.onSetStepStatus}
        />
      </>
    )
  }
}

export default DataStreaming
