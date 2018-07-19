import React, {SFC} from 'react'

import AlertTabs from 'src/kapacitor/components/AlertTabs'

import {Kapacitor, Source, Notification, NotificationFunc} from 'src/types'

interface AlertOutputProps {
  exists: boolean
  kapacitor: Kapacitor
  source: Source
  hash: string
  notify: (message: Notification | NotificationFunc) => void
}

const AlertOutputs: SFC<AlertOutputProps> = ({
  hash,
  exists,
  source,
  kapacitor,
  notify,
}) => {
  if (exists) {
    return (
      <AlertTabs
        hash={hash}
        source={source}
        kapacitor={kapacitor}
        notify={notify}
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
