import React, {PropTypes} from 'react'
import {formatRPDuration} from 'utils/formatting'

export const DatabaseRow = ({name, duration, replication, isDefault}) => {
  return (
    <tr>
      <td>
        {name}
        {isDefault ? <span className="default-source-label">default</span> : null}
      </td>
      <td>{formatRPDuration(duration)}</td>
      <td>{replication}</td>
      <td className="text-right">
        <button className="btn btn-xs btn-danger admin-table--delete">
          {`Delete ${name}`}
        </button>
      </td>
    </tr>
  )
}

const {
  bool,
  number,
  string,
} = PropTypes

DatabaseRow.propTypes = {
  name: string,
  duration: string,
  replication: number,
  isDefault: bool,
}
