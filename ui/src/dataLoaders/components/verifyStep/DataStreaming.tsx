// Libraries
import React, {PureComponent} from 'react'
import _ from 'lodash'

// Components
import TelegrafInstructions from 'src/dataLoaders/components/verifyStep/TelegrafInstructions'
import CreateOrUpdateConfig from 'src/dataLoaders/components/verifyStep/CreateOrUpdateConfig'
import DataListening from 'src/dataLoaders/components/verifyStep/DataListening'

// Decorator
import {ErrorHandling} from 'src/shared/decorators/errors'

// Types
import {NotificationAction} from 'src/types'

interface Props {
  notify: NotificationAction
  bucket: string
  org: string
  configID: string
  authToken: string
}

@ErrorHandling
class DataStreaming extends PureComponent<Props> {
  public render() {
    const {authToken, org, configID, bucket, notify} = this.props

    return (
      <div className="streaming">
        <CreateOrUpdateConfig org={org}>
          {() => (
            <TelegrafInstructions
              notify={notify}
              authToken={authToken}
              configID={configID}
            />
          )}
        </CreateOrUpdateConfig>

        <DataListening bucket={bucket} />
      </div>
    )
  }
}

export default DataStreaming
