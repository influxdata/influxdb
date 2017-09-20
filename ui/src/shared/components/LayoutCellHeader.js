import React, {PropTypes} from 'react'
import classnames from 'classnames'

import CustomTimeIndicator from 'shared/components/CustomTimeIndicator'

import {NEW_DEFAULT_DASHBOARD_CELL} from 'src/dashboards/constants/index'

const LayoutCellHeader = ({queries, isEditable, cellName}) => {
  const cellNameIsDefault = cellName === NEW_DEFAULT_DASHBOARD_CELL.name

  return (
    <div
      className={classnames('dash-graph--heading', {
        'dash-graph--heading-draggable': isEditable,
      })}
    >
      <span
        className={
          cellNameIsDefault
            ? 'dash-graph--name dash-graph--name__default'
            : 'dash-graph--name'
        }
      >
        {cellName}
        {queries && queries.length
          ? <CustomTimeIndicator queries={queries} />
          : null}
      </span>
    </div>
  )
}

const {array, bool, string} = PropTypes

LayoutCellHeader.propTypes = {
  queries: array,
  isEditable: bool,
  cellName: string,
}

export default LayoutCellHeader
