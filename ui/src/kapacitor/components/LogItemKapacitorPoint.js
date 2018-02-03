import React, {Component, PropTypes} from 'react'

class LogItemKapacitorPoint extends Component {
  renderKeysAndValues = (object, name) => {
    if (!object) {
      return <span className="logs-table--empty-cell">--</span>
    }
    const objKeys = Object.keys(object)
    const objValues = Object.values(object)

    if (objKeys.length > 2) {
      return (
        <div className="logs-table--many-keys">
          {`${objKeys.length} ${name}...`}
        </div>
      )
    }

    const objElements = objKeys.map((objKey, i) =>
      <div key={i} className="logs-table--key-value">
        {objKey}: <span>{objValues[i]}</span>
      </div>
    )
    return objElements
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
        {this.isExpandable() &&
          <div className="logs-table--row-expander">
            Click to show all items
          </div>}
        <div className="logs-table--divider">
          <div className={`logs-table--level ${logItem.lvl}`} />
          <div className="logs-table--timestamp">
            {logItem.ts}
          </div>
        </div>
        <div className="logs-table--details">
          <div className="logs-table--service">Kapacitor Point</div>
          <div className="logs-table--blah">
            <div className="logs-table--key-values">
              <h1>TAGS</h1>
              {this.renderKeysAndValues(logItem.tag, 'Tags')}
            </div>
            <div className="logs-table--key-values">
              <h1>FIELDS</h1>
              {this.renderKeysAndValues(logItem.field, 'Fields')}
            </div>
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
