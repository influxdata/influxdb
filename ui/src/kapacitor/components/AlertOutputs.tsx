import React, {SFC} from 'react'

import AlertTabs from 'src/kapacitor/components/AlertTabs'

import {Kapacitor, Source} from 'src/types'

interface AlertOutPutProps {
  exists: boolean
  kapacitor: Kapacitor
  addFlashMessage: () => void
  source: Source
  hash: string
}

const AlertOutputs: SFC<AlertOutPutProps> = ({
  exists,
  kapacitor,
  addFlashMessage,
  source,
  hash,
}) => {
  if (exists) {
    return (
      <AlertTabs
        source={source}
        kapacitor={kapacitor}
        addFlashMessage={addFlashMessage}
        hash={hash}
      />
    )
  }

  return (
    <div className="panel">
      <div className="panel-heading">
        <h2 className="panel-title">Configure Alert Endpoints</h2>
      </div>
      <div className="panel-body">
        <div className="generic-empty-state">
          <h4 className="no-user-select">
            Connect to an active Kapacitor instance to configure alerting
            endpoints
          </h4>
        </div>
      </div>
    </div>
  )
}

export default AlertOutputs
