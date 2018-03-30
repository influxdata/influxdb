import React from 'react'
import PropTypes from 'prop-types'

import ConfirmButton from 'shared/components/ConfirmButton'
import {QUERIES_TABLE} from 'src/admin/constants/tableSizing'

const QueryRow = ({query, onKill}) => {
  const {database, duration} = query
  const wrappedKill = () => {
    onKill(query.id)
  }

  return (
    <tr>
      <td
        style={{width: `${QUERIES_TABLE.colDatabase}px`}}
        className="monotype"
      >
        {database}
      </td>
      <td>
        <code>{query.query}</code>
      </td>
      <td style={{width: `${QUERIES_TABLE.colRunning}px`}} className="monotype">
        {duration}
      </td>
      <td
        style={{width: `${QUERIES_TABLE.colKillQuery}px`}}
        className="text-right"
      >
        <ConfirmButton
          text="Kill"
          confirmAction={wrappedKill}
          size="btn-xs"
          type="btn-danger"
          customClass="table--show-on-row-hover"
        />
      </td>
    </tr>
  )
}

const {func, shape} = PropTypes

QueryRow.propTypes = {
  query: shape().isRequired,
  onKill: func.isRequired,
}

export default QueryRow
