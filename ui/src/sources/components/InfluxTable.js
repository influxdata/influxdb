import React, {PropTypes} from 'react'
import {Link} from 'react-router'

const InfluxTable = ({
  sources,
  source,
  handleDeleteSource,
  kapacitors,
  location,
}) => (
  <div className="row">
    <div className="col-md-12">
      <div className="panel panel-minimal">
        <div className="panel-heading u-flex u-ai-center u-jc-space-between">
          <h2 className="panel-title">InfluxDB Sources</h2>
          <Link to={`/sources/${source.id}/manage-sources/new`} className="btn btn-sm btn-primary">Add New Source</Link>
        </div>
        <div className="panel-body">
          <div className="table-responsive margin-bottom-zero">
            <table className="table v-center margin-bottom-zero">
              <thead>
                <tr>
                  <th>Name</th>
                  <th>Host</th>
                  <th>Kapacitor</th>
                  <th className="text-right"></th>
                </tr>
              </thead>
              <tbody>
                {
                  sources.map((s) => {
                    const kapacitorName = kapacitors[s.id] ? kapacitors[s.id].name : ''
                    return (
                      <tr key={s.id}>
                        <td>{s.name}{s.default ? <span className="default-source-label">Default</span> : null}</td>
                        <td className="monotype">{s.url}</td>
                        <td>{kapacitorName ? kapacitorName : "--"}</td>
                        <td className="text-right">
                          <Link className="btn btn-info btn-xs" to={`${location.pathname}/${s.id}/edit`}><span className="icon pencil"></span></Link>
                          <Link className="btn btn-success btn-xs" to={`/sources/${s.id}/hosts`}>Connect</Link>
                          <button className="btn btn-danger btn-xs" onClick={() => handleDeleteSource(s)}><span className="icon trash"></span></button>
                        </td>
                      </tr>
                    )
                  })
                }
              </tbody>
            </table>
          </div>
        </div>
      </div>
    </div>
  </div>
)

const {
  array,
  func,
  object,
  shape,
  string,
} = PropTypes

InfluxTable.propTypes = {
  sources: array.isRequired,
  kapacitors: object,
  location: shape({
    pathname: string.isRequired,
  }).isRequired,
  handleDeleteSource: func.isRequired,
  source: shape({
    id: string.isRequired,
    links: shape({
      proxy: string.isRequired,
      self: string.isRequired,
    }),
  }),
}

export default InfluxTable
