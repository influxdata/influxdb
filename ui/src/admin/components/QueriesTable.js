import React, {PropTypes} from 'react'

const QueriesTable = ({queries, onKillQuery, onConfirm}) => (
  <div>
    <div className="panel panel-minimal">
      <div className="panel-body">
        <table className="table v-center">
          <thead>
            <tr>
              <th>Database</th>
              <th>Query</th>
              <th>Running</th>
              <th></th>
            </tr>
          </thead>
          <tbody>
            {queries.map((q) => {
              return (
                <tr key={q.id}>
                  <td>{q.database}</td>
                  <td><code>{q.query}</code></td>
                  <td>{q.duration}</td>
                  <td className="text-right">
                    <button className="btn btn-xs btn-link-danger" onClick={onKillQuery} data-toggle="modal" data-query-id={q.id} data-target="#killModal">
                      Kill
                    </button>
                  </td>
                </tr>
              )
            })}
          </tbody>
        </table>
      </div>
    </div>

    <div className="modal fade" id="killModal" tabIndex="-1" role="dialog" aria-labelledby="myModalLabel">
      <div className="modal-dialog" role="document">
        <div className="modal-content">
          <div className="modal-header">
            <button type="button" className="close" data-dismiss="modal" aria-label="Close">
              <span aria-hidden="true">×</span>
            </button>
            <h4 className="modal-title" id="myModalLabel">Are you sure you want to kill this query?</h4>
          </div>
          <div className="modal-footer">
            <button type="button" className="btn btn-default" data-dismiss="modal">No</button>
            <button type="button" className="btn btn-danger" data-dismiss="modal" onClick={onConfirm}>Yes, kill it!</button>
          </div>
        </div>
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
