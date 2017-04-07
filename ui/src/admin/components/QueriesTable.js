import React, {PropTypes} from 'react'

import QueryRow from 'src/admin/components/QueryRow'

const QueriesTable = ({queries, onKillQuery}) => (
  <div>
    <div className="panel panel-minimal">
      <div className="panel-body">
        <table className="table v-center admin-table">
          <thead>
            <tr>
              <th>Database</th>
              <th>Query</th>
              <th>Running</th>
              <th></th>
            </tr>
          </thead>
          <tbody>
            {queries.map((q) => <QueryRow key={q.id} query={q} onKill={onKillQuery}/>)}
          </tbody>
        </table>
      </div>
    </div>
  </div>
)

const {
  arrayOf,
  func,
  shape,
} = PropTypes

QueriesTable.propTypes = {
  queries: arrayOf(shape()),
  onConfirm: func,
  onKillQuery: func,
}

export default QueriesTable
