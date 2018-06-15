import React, {SFC} from 'react'

import {LogItem} from 'src/types/kapacitor'

interface Props {
  logItem: LogItem
}

const LogItemKapacitorDebug: SFC<Props> = ({logItem}) => (
  <div className="logs-table--row">
    <div className="logs-table--divider">
      <div className={`logs-table--level ${logItem.lvl}`} />
      <div className="logs-table--timestamp">{logItem.ts}</div>
    </div>
    <div className="logs-table--details">
      <div className="logs-table--service debug">Kapacitor</div>
      <div className="logs-table--columns">
        <div className="logs-table--key-values debug">DEBUG: {logItem.msg}</div>
      </div>
    </div>
  </div>
)

export default LogItemKapacitorDebug
