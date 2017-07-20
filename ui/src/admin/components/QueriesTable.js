import React, {PropTypes} from 'react'

import QueryRow from 'src/admin/components/QueryRow'
import {QUERIES_TABLE} from 'src/admin/constants/tableSizing'

const QueriesTable = ({queries, onKillQuery}) =>
  <div>
    <div className="panel panel-default">
      <div className="panel-body">
        <table className="table v-center admin-table table-highlight">
          <thead>
            <tr>
              <th style={{width: `${QUERIES_TABLE.colDatabase}px`}}>
                Database
              </th>
              <th>Query</th>
              <th style={{width: `${QUERIES_TABLE.colRunning}px`}}>Running</th>
              <th style={{width: `${QUERIES_TABLE.colKillQuery}px`}} />
            </tr>
          </thead>
          <tbody>
            {queries.map(q =>
              <QueryRow key={q.id} query={q} onKill={onKillQuery} />
            )}
          </tbody>
        </table>
      </div>
    </div>
  </div>

const {arrayOf, func, shape} = PropTypes

QueriesTable.propTypes = {
  queries: arrayOf(shape()),
  onConfirm: func,
  onKillQuery: func,
}

export default QueriesTable
