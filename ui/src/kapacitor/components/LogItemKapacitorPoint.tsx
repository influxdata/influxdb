import React, {PureComponent} from 'react'
import _ from 'lodash'

import {ErrorHandling} from 'src/shared/decorators/errors'

import {LogItem} from 'src/types/kapacitor'

interface Props {
  logItem: LogItem
}

@ErrorHandling
class LogItemKapacitorPoint extends PureComponent<Props> {
  public render() {
    const {logItem} = this.props
    return (
      <div className="logs-table--row">
        <div className="logs-table--divider">
          <div className={`logs-table--level ${logItem.lvl}`} />
          <div className="logs-table--timestamp">{logItem.ts}</div>
        </div>
        <div className="logs-table--details">
          <div className="logs-table--service">Kapacitor Point</div>
          <div className="logs-table--columns">
            {this.renderKeysAndValues(logItem.tag, 'Tags')}
            {this.renderKeysAndValues(logItem.field, 'Fields')}
          </div>
        </div>
      </div>
    )
  }

  private renderKeysAndValues = (object: any, name: string) => {
    if (_.isEmpty(object)) {
      return <span className="logs-table--empty-cell">--</span>
    }

    const sortedObjKeys = Object.keys(object).sort()

    return (
      <div className="logs-table--column">
        <h1>{`${sortedObjKeys.length} ${name}`}</h1>
        <div className="logs-table--scrollbox">
          {sortedObjKeys.map(objKey => (
            <div key={objKey} className="logs-table--key-value">
              {objKey}: <span>{object[objKey]}</span>
            </div>
          ))}
        </div>
      </div>
    )
  }
}

export default LogItemKapacitorPoint
