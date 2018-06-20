import React, {SFC} from 'react'

import {LogItem} from 'src/types/kapacitor'

interface Props {
  logItem: LogItem
}

const LogItemSession: SFC<Props> = ({logItem}) => (
  <div className="logs-table--row">
    <div className="logs-table--divider">
      <div className={`logs-table--level ${logItem.lvl}`} />
      <div className="logs-table--timestamp">{logItem.ts}</div>
    </div>
    <div className="logs-table--details">
      <div className="logs-table--session">{logItem.msg}</div>
    </div>
  </div>
)

export default LogItemSession
