import React, {PropTypes} from 'react'
import _ from 'lodash'

import CustomTimeIndicator from 'shared/components/CustomTimeIndicator'

import {NEW_DEFAULT_DASHBOARD_CELL} from 'src/dashboards/constants/index'

const LayoutCellHeader = (
  {queries, isEditable, cellName, sources = []},
  {source: defaultSource}
) => {
  const cellNameIsDefault = cellName === NEW_DEFAULT_DASHBOARD_CELL.name
  const querySource = sources.find(
    s => s.links.self === _.get(queries, ['0', 'source'], null)
  )

  const headingClass = `dash-graph--heading ${isEditable
    ? 'dash-graph--heading-draggable'
    : ''}`

  return (
    <div className={headingClass}>
      <span
        className={
          cellNameIsDefault
            ? 'dash-graph--name dash-graph--name__default'
            : 'dash-graph--name'
        }
      >
        {querySource && querySource.id !== defaultSource.id
          ? <span className="dash-graph--custom-source">
              {querySource.name}
            </span>
          : null}
        {cellName}
        {queries && queries.length
          ? <CustomTimeIndicator queries={queries} />
          : null}
      </span>
    </div>
  )
}

const {arrayOf, bool, shape, string} = PropTypes

LayoutCellHeader.contextTypes = {
  source: shape({}),
}

LayoutCellHeader.propTypes = {
  queries: arrayOf(shape()),
  isEditable: bool,
  cellName: string,
  sources: arrayOf(shape()),
}

export default LayoutCellHeader
