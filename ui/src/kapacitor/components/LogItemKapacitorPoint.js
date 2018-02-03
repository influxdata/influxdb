import React, {Component, PropTypes} from 'react'

class LogItemKapacitorPoint extends Component {
  renderKeysAndValues = (object, name, expanded) => {
    if (!object) {
      return <span className="logs-table--empty-cell">--</span>
    }
    const objKeys = Object.keys(object)
    const objValues = Object.values(object)

    if (objKeys.length > 2) {
      return expanded
        ? <div className="logs-table--key-values">
            <h1>
              {`${objKeys.length} ${name}`}
            </h1>
            <div className="logs-table--keys-scrollbox">
              {objKeys.map((objKey, i) =>
                <div key={i} className="logs-table--key-value">
                  {objKey}: <span>{objValues[i]}</span>
                </div>
              )}
            </div>
          </div>
        : <div className="logs-table--key-values">
            <h1>
              {`${objKeys.length} ${name}`}
            </h1>
            <div className="logs-table--many-keys">Click to expand...</div>
          </div>
    }

    return (
      <div className="logs-table--key-values">
        <h1>
          {`${objKeys.length} ${name}`}
        </h1>
        {objKeys.map((objKey, i) =>
          <div key={i} className="logs-table--key-value">
            {objKey}: <span>{objValues[i]}</span>
          </div>
        )}
      </div>
    )
  }

  handleToggleExpand = () => {
    const {onToggleExpandLog, logIndex} = this.props

    if (this.isExpandable()) {
      onToggleExpandLog(logIndex)
    }
  }

  isExpandable = () => {
    const {logItem} = this.props

    if (
      Object.keys(logItem.tag).length > 2 ||
      Object.keys(logItem.field).length > 2
    ) {
      return true
    }
    return false
  }

  render() {
    const {logItem} = this.props

    const rowClass = `logs-table--row${this.isExpandable()
      ? ' logs-table--row__expandable'
      : ''}${logItem.expanded ? ' expanded' : ''}`

    return (
      <div className={rowClass} onClick={this.handleToggleExpand}>
        <div className="logs-table--divider">
          <div className={`logs-table--level ${logItem.lvl}`} />
          <div className="logs-table--timestamp">
            {logItem.ts}
          </div>
        </div>
        <div className="logs-table--details">
          <div className="logs-table--service">Kapacitor Point</div>
          <div className="logs-table--blah">
            {this.renderKeysAndValues(logItem.tag, 'Tags', logItem.expanded)}
            {this.renderKeysAndValues(
              logItem.field,
              'Fields',
              logItem.expanded
            )}
          </div>
        </div>
      </div>
    )
  }
}

const {func, number, shape, string} = PropTypes

LogItemKapacitorPoint.propTypes = {
  logItem: shape({
    lvl: string.isRequired,
    ts: string.isRequired,
    tag: shape.isRequired,
    field: shape.isRequired,
  }),
  onToggleExpandLog: func.isRequired,
  logIndex: number.isRequired,
}

export default LogItemKapacitorPoint
