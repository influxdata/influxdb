// Libraries
import React, {PureComponent} from 'react'
import _ from 'lodash'

// Components
import TelegrafInstructions from 'src/dataLoaders/components/verifyStep/TelegrafInstructions'
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
  token: string
}

@ErrorHandling
class DataStreaming extends PureComponent<Props> {
  public render() {
    const {token, configID, bucket, notify} = this.props

    return (
      <div className="streaming">
        <TelegrafInstructions
          notify={notify}
          token={token}
          configID={configID}
        />

        <DataListening bucket={bucket} />
      </div>
    )
  }
}

export default DataStreaming
