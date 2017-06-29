import React, {PropTypes} from 'react'
import _ from 'lodash'

const removeMeasurement = (label = '') => {
  const [measurement] = label.match(/^(.*)[.]/g) || ['']
  return label.replace(measurement, '')
}

const DygraphLegend = ({
  series,
  onSort,
  onSnip,
  onHide,
  isHidden,
  isSnipped,
  sortType,
  legendRef,
  filterText,
  isAscending,
  onInputChange,
}) => {
  const sorted = _.sortBy(
    series,
    ({y, label}) => (sortType === 'numeric' ? y : label)
  )
  const ordered = isAscending ? sorted : sorted.reverse()
  const filtered = ordered.filter(s => s.label.match(filterText))
  const hidden = isHidden ? 'hidden' : ''

  return (
    <div
      style={{
        userSelect: 'text',
        transform: 'translate(-50%)',
      }}
      className={`container--dygraph-legend ${hidden}`}
      ref={legendRef}
      onMouseLeave={onHide}
    >
      <div className="dygraph-legend--header">
        <input
          className="form-control input-xs"
          type="text"
          value={filterText}
          onChange={onInputChange}
        />
        <button
          className="btn btn-primary btn-xs"
          onClick={() => onSort('alphabetic')}
        >
          A-Z
        </button>
        <button
          className="btn btn-primary btn-xs"
          onClick={() => onSort('numeric')}
        >
          0-9
        </button>
        <button className="btn btn-primary btn-xs" onClick={onSnip}>
          Snip Measurement
        </button>
      </div>
      <div className="dygraph-legend--contents">
        {filtered.map(({label, color, yHTML, isHighlighted}) => {
          return (
            <span key={label + color}>
              <b>
                <span
                  style={{color, fontWeight: isHighlighted ? 'bold' : 'normal'}}
                >
                  {isSnipped ? removeMeasurement(label) : label}
                  :
                  {' '}
                  {yHTML || 'no value'}
                </span>
              </b>
            </span>
          )
        })}
      </div>
    </div>
  )
}

const {arrayOf, bool, func, number, shape, string} = PropTypes

DygraphLegend.propTypes = {
  x: number,
  xHTML: string,
  series: arrayOf(
    shape({
      color: string,
      dashHTML: string,
      isVisible: bool,
      label: string,
      y: number,
      yHTML: string,
    })
  ),
  dygraph: shape(),
  onSnip: func.isRequired,
  onHide: func.isRequired,
  onSort: func.isRequired,
  onInputChange: func.isRequired,
  filterText: string.isRequired,
  isAscending: bool.isRequired,
  sortType: string.isRequired,
  isHidden: bool.isRequired,
  legendRef: func.isRequired,
  isSnipped: bool.isRequired,
}

export default DygraphLegend
