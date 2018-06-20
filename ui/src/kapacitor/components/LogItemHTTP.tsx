import React, {SFC} from 'react'

import {LogItem} from 'src/types/kapacitor'

interface Props {
  logItem: LogItem
}

const LogItemHTTP: SFC<Props> = ({logItem}) => (
  <div className="logs-table--row">
    <div className="logs-table--divider">
      <div className={`logs-table--level ${logItem.lvl}`} />
      <div className="logs-table--timestamp">{logItem.ts}</div>
    </div>
    <div className="logs-table--details">
      <div className="logs-table--service">HTTP Request</div>
      <div className="logs-table--http">
        {logItem.method} {logItem.username}@{logItem.host} ({logItem.duration})
      </div>
    </div>
  </div>
)

export default LogItemHTTP
