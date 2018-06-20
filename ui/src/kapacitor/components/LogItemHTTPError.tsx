import React, {SFC} from 'react'

import {LogItem} from 'src/types/kapacitor'

interface Props {
  logItem: LogItem
}

const LogItemHTTPError: SFC<Props> = ({logItem}) => (
  <div className="logs-table--row" key={logItem.key}>
    <div className="logs-table--divider">
      <div className={`logs-table--level ${logItem.lvl}`} />
      <div className="logs-table--timestamp">{logItem.ts}</div>
    </div>
    <div className="logs-table--details">
      <div className="logs-table--service error">HTTP Server</div>
      <div className="logs-table--columns">
        <div className="logs-table--key-values error">ERROR: {logItem.msg}</div>
      </div>
    </div>
  </div>
)

export default LogItemHTTPError
