import React, {PropTypes} from 'react'

const renderKeysAndValues = (object, name) => {
  if (!object) {
    return <span className="logs-table--empty-cell">--</span>
  }
  const objKeys = Object.keys(object)
  const objValues = Object.values(object)

  return (
    <div className="logs-table--column">
      <h1>
        {`${objKeys.length} ${name}`}
      </h1>
      <div className="logs-table--scrollbox">
        {objKeys.map((objKey, i) =>
          <div key={i} className="logs-table--key-value">
            {objKey}: <span>{objValues[i]}</span>
          </div>
        )}
      </div>
    </div>
  )
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
      <div className="logs-table--columns">
        {renderKeysAndValues(logItem.tag, 'Tags')}
        {renderKeysAndValues(logItem.field, 'Fields')}
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
