import React, {PropTypes} from 'react'
import _ from 'lodash'
import classnames from 'classnames'
import uuid from 'node-uuid'

import {makeLegendStyles} from 'shared/graphs/helpers'

const removeMeasurement = (label = '') => {
  const [measurement] = label.match(/^(.*)[.]/g) || ['']
  return label.replace(measurement, '')
}

const DygraphLegend = ({
  xHTML,
  pageX,
  graph,
  legend,
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
  isFilterVisible,
  onToggleFilter,
}) => {
  const withValues = series.filter(s => s.y >= 0 || s.y <= 0)
  const sorted = _.sortBy(
    withValues,
    ({y, label}) => (sortType === 'numeric' ? y : label)
  )

  const ordered = isAscending ? sorted : sorted.reverse()
  const filtered = ordered.filter(s => s.label.match(filterText))
  const hidden = isHidden ? 'hidden' : ''
  const style = makeLegendStyles(graph, legend, pageX)

  const renderSortAlpha = (
    <div
      className={classnames('sort-btn btn btn-sm btn-square', {
        'btn-primary': sortType !== 'numeric',
        'btn-default': sortType === 'numeric',
        'sort-btn--asc': isAscending && sortType !== 'numeric',
        'sort-btn--desc': !isAscending && sortType !== 'numeric',
      })}
      onClick={onSort('alphabetic')}
    >
      <div className="sort-btn--arrow" />
      <div className="sort-btn--top">A</div>
      <div className="sort-btn--bottom">Z</div>
    </div>
  )
  const renderSortNum = (
    <button
      className={classnames('sort-btn btn btn-sm btn-square', {
        'btn-primary': sortType === 'numeric',
        'btn-default': sortType !== 'numeric',
        'sort-btn--asc': isAscending && sortType === 'numeric',
        'sort-btn--desc': !isAscending && sortType === 'numeric',
      })}
      onClick={onSort('numeric')}
    >
      <div className="sort-btn--arrow" />
      <div className="sort-btn--top">0</div>
      <div className="sort-btn--bottom">9</div>
    </button>
  )
  return (
    <div
      className={`dygraph-legend ${hidden}`}
      ref={legendRef}
      onMouseLeave={onHide}
      style={style}
    >
      <div className="dygraph-legend--header">
        <div className="dygraph-legend--timestamp">
          {xHTML}
        </div>
        {renderSortAlpha}
        {renderSortNum}
        <button
          className={classnames('btn btn-square btn-sm', {
            'btn-default': !isFilterVisible,
            'btn-primary': isFilterVisible,
          })}
          onClick={onToggleFilter}
        >
          <span className="icon search" />
        </button>
        <button
          className={classnames('btn btn-sm', {
            'btn-default': !isSnipped,
            'btn-primary': isSnipped,
          })}
          onClick={onSnip}
        >
          Snip
        </button>
      </div>
      {isFilterVisible
        ? <input
            className="dygraph-legend--filter form-control input-sm"
            type="text"
            value={filterText}
            onChange={onInputChange}
            placeholder="Filter items..."
            autoFocus={true}
          />
        : null}
      <div className="dygraph-legend--divider" />
      <div className="dygraph-legend--contents">
        {filtered.map(({label, color, yHTML, isHighlighted}) => {
          const seriesClass = isHighlighted
            ? 'dygraph-legend--row highlight'
            : 'dygraph-legend--row'
          return (
            <div key={uuid.v4()} className={seriesClass}>
              <span style={{color}}>
                {isSnipped ? removeMeasurement(label) : label}
              </span>
              <figure>
                {yHTML || 'no value'}
              </figure>
            </div>
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
  pageX: number,
  legend: shape({}),
  graph: shape({}),
  onSnip: func.isRequired,
  onHide: func.isRequired,
  onSort: func.isRequired,
  onInputChange: func.isRequired,
  onToggleFilter: func.isRequired,
  filterText: string.isRequired,
  isAscending: bool.isRequired,
  sortType: string.isRequired,
  isHidden: bool.isRequired,
  legendRef: func.isRequired,
  isSnipped: bool.isRequired,
  isFilterVisible: bool.isRequired,
}

export default DygraphLegend
