import React, {PropTypes} from 'react'
import _ from 'lodash'

const DygraphLegend = ({
  series,
  onSort,
  isHidden,
  legendRef,
  filterText,
  onInputChange,
  sortOrder,
  sortType,
}) => {
  const sorted = _.sortBy(
    series,
    ({y, label}) => (sortType === 'numeric' ? y : label)
  )
  const ordered = sortOrder === 'desc' ? sorted.reverse() : sorted
  const filtered = ordered.filter(s => s.label.match(filterText))
  const hidden = isHidden ? 'hidden' : ''

  return (
    <div
      style={{
        userSelect: 'text',
        transform: 'translate(-50%)',
        overflowY: 'scroll',
      }}
      className={`container--dygraph-legend ${hidden}`}
      ref={legendRef}
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
      </div>
      <div className="dygraph-legend--contents">
        {filtered.map(({label, color, yHTML, isHighlighted}) => {
          return (
            <span key={label + color}>
              <b>
                <span
                  style={{color, fontWeight: isHighlighted ? 'bold' : 'normal'}}
                >
                  {label}: {yHTML || 'no value'}
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
  onSort: func.isRequired,
  onInputChange: func.isRequired,
  filterText: string.isRequired,
  sortOrder: string.isRequired,
  sortType: string.isRequired,
  isHidden: bool.isRequired,
  legendRef: func.isRequired,
}

export default DygraphLegend
