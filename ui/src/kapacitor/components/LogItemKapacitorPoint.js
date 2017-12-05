import React, {PropTypes} from 'react'

const renderKeysAndValues = object => {
  if (!object) {
    return <span className="logs-table--empty-cell">--</span>
  }
  const objKeys = Object.keys(object)
  const objValues = Object.values(object)

  const objElements = objKeys.map((objKey, i) =>
    <div key={i} className="logs-table--key-value">
      {objKey}: <span>{objValues[i]}</span>
    </div>
  )
  return objElements
}
const LogItemKapacitorPoint = ({logItem}) =>
  <div className="logs-table--row">
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
          TAGS<br />
          {renderKeysAndValues(logItem.tag)}
        </div>
        <div className="logs-table--key-values">
          FIELDS<br />
          {renderKeysAndValues(logItem.field)}
        </div>
      </div>
    </div>
  </div>

const {shape, string} = PropTypes

LogItemKapacitorPoint.propTypes = {
  logItem: shape({
    lvl: string.isRequired,
    ts: string.isRequired,
    tag: shape.isRequired,
    field: shape.isRequired,
  }),
}

export default LogItemKapacitorPoint
