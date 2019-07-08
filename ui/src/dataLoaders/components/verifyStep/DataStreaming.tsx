// Libraries
import React, {PureComponent} from 'react'
import _ from 'lodash'

// Components
import TelegrafInstructions from 'src/dataLoaders/components/verifyStep/TelegrafInstructions'
import DataListening from 'src/dataLoaders/components/verifyStep/DataListening'

// Decorator
import {ErrorHandling} from 'src/shared/decorators/errors'

interface Props {
  bucket: string
  org: string
  configID: string
  token: string
}

@ErrorHandling
class DataStreaming extends PureComponent<Props> {
  public render() {
    const {token, configID, bucket} = this.props

    return (
      <div className="streaming">
        <TelegrafInstructions token={token} configID={configID} />

        <DataListening bucket={bucket} />
      </div>
    )
  }
}

export default DataStreaming
