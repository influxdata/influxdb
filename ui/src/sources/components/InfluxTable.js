import React, {PropTypes} from 'react'
import {Link} from 'react-router'

import Dropdown from 'shared/components/Dropdown'

const kapacitorDropdown = (kapacitors, source) => {
  if (!kapacitors || kapacitors.length === 0) {
    return (
      <span>--</span>
    )
  }
  const kapacitorItems = kapacitors.map((k) => {
    return {text: k.name}
  })
  return (
    <Dropdown
      items={kapacitorItems}
      onChoose={() => {}}
      addNew={{
        url: `/sources/${source.id}/kapacitors/new`,
        text: "Add Kapacitor",
      }}
      selected={kapacitorItems[0].text}
    />
  )
}

const InfluxTable = ({
  sources,
  source,
  handleDeleteSource,
  location,
}) => (
  <div className="row">
    <div className="col-md-12">
      <div className="panel panel-minimal">
        <div className="panel-heading u-flex u-ai-center u-jc-space-between">
          <h2 className="panel-title">InfluxDB Sources</h2>
          <Link to={`/sources/${source.id}/manage-sources/new`} className="btn btn-sm btn-primary">Add Source</Link>
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
                    return (
                      <tr key={s.id}>
                        <td>{s.name}{s.default ? <span className="default-source-label">Default</span> : null}</td>
                        <td className="monotype">{s.url}</td>
                        <td>
                          {
                            kapacitorDropdown(s.kapacitors, source)
                          }
                        </td>
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
  shape,
  string,
} = PropTypes

InfluxTable.propTypes = {
  sources: array.isRequired,
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
