import React, {PropTypes} from 'react'
import {Link} from 'react-router'

const KapacitorTable = () => (
  <div className="row">
    <div className="col-md-12">
      <div className="panel panel-minimal">
        <div className="panel-heading u-flex u-ai-center u-jc-space-between">
          <h2 className="panel-title">Kapacitor Nodes</h2>
          <Link to={"#"} className="btn btn-sm btn-success">Add Node</Link>
        </div>
        <div className="panel-body">
          <div className="table-responsive margin-bottom-zero">
            <table className="table v-center margin-bottom-zero">
              <thead>
                <tr>
                  <th>Name</th>
                  <th>URL</th>
                  <th>Configured Endpoints</th>
                  <th className="text-right"></th>
                </tr>
              </thead>
              <tbody>
              </tbody>
            </table>
          </div>
        </div>
      </div>
    </div>
  </div>
)

// const {
//   array,
// } = PropTypes
//
// KapacitorTable.propTypes = {
//   kapacitors: array.isRequired,
// }

export default KapacitorTable
