// Libraries
import React, {PureComponent} from 'react'
import _ from 'lodash'

// Components
import TelegrafInstructions from 'src/onboarding/components/verifyStep/TelegrafInstructions'
import CreateOrUpdateConfig from 'src/onboarding/components/verifyStep/CreateOrUpdateConfig'
import DataListening from 'src/onboarding/components/verifyStep/DataListening'

// Actions
import {createOrUpdateTelegrafConfigAsync} from 'src/onboarding/actions/dataLoaders'

// Constants
import {StepStatus} from 'src/clockface/constants/wizard'

// Decorator
import {ErrorHandling} from 'src/shared/decorators/errors'

// Types
import {NotificationAction} from 'src/types'

interface Props {
  notify: NotificationAction
  bucket: string
  org: string
  configID: string
  stepIndex: number
  authToken: string
  onSetStepStatus: (index: number, status: StepStatus) => void
  onSaveTelegrafConfig: typeof createOrUpdateTelegrafConfigAsync
}

@ErrorHandling
class DataStreaming extends PureComponent<Props> {
  public render() {
    const {
      authToken,
      org,
      configID,
      onSaveTelegrafConfig,
      onSetStepStatus,
      bucket,
      stepIndex,
      notify,
    } = this.props

    return (
      <>
        <CreateOrUpdateConfig
          org={org}
          notify={notify}
          authToken={authToken}
          onSaveTelegrafConfig={onSaveTelegrafConfig}
        >
          {() => (
            <TelegrafInstructions
              notify={notify}
              authToken={authToken}
              configID={configID}
            />
          )}
        </CreateOrUpdateConfig>

        <DataListening
          bucket={bucket}
          stepIndex={stepIndex}
          onSetStepStatus={onSetStepStatus}
        />
      </>
    )
  }
}

export default DataStreaming
