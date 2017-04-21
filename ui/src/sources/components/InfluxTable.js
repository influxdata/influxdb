import React, {PropTypes} from 'react'
import {Link, withRouter} from 'react-router'

import Dropdown from 'shared/components/Dropdown'

const kapacitorDropdown = (kapacitors, source, router) => {
  if (!kapacitors || kapacitors.length === 0) {
    return (
      <Link to={`/sources/${source.id}/kapacitors/new`}>Add Kapacitor</Link>
    )
  }
  const kapacitorItems = kapacitors.map((k) => {
    return {text: k.name, resource: `/sources/${source.id}/kapacitors/${k.id}`}
  })

  const activeKapacitor = kapacitors.find((k) => k.active)

  let selected = ''
  if (activeKapacitor) {
    selected = activeKapacitor.name
  } else {
    selected = kapacitorItems[0].text
  }

  return (
    <Dropdown
      className="sources--kapacitor-selector"
      buttonColor="btn-info"
      buttonSize="btn-xs"
      items={kapacitorItems}
      onChoose={() => {}}
      addNew={{
        url: `/sources/${source.id}/kapacitors/new`,
        text: "Add Kapacitor",
      }}
      actions={
      [{
        icon: "pencil",
        text: "edit",
        handler: (item) => {
          router.push(`${item.resource}/edit`)
        },
      }]}
      selected={selected}
    />
  )
}

const InfluxTable = ({
  sources,
  source,
  handleDeleteSource,
  location,
  router,
}) => (
  <div className="row">
    <div className="col-md-12">
      <div className="panel panel-minimal">
        <div className="panel-heading u-flex u-ai-center u-jc-space-between">
          <h2 className="panel-title">InfluxDB Sources</h2>
          <Link to={`/sources/${source.id}/manage-sources/new`} className="btn btn-sm btn-primary">Add Source</Link>
        </div>
        <div className="panel-body">
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
                      <td><Link to={`${location.pathname}/${s.id}/edit`}>{s.name}</Link> {s.default ? <span className="default-source-label">Default</span> : null}</td>
                      <td className="monotype">{s.url}</td>
                      <td>
                        {
                          kapacitorDropdown(s.kapacitors, s, router)
                        }
                      </td>
                      <td className="text-right">
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
)

const {
  array,
  func,
  shape,
  string,
} = PropTypes

InfluxTable.propTypes = {
  handleDeleteSource: func.isRequired,
  location: shape({
    pathname: string.isRequired,
  }).isRequired,
  router: PropTypes.shape({
    push: PropTypes.func.isRequired,
  }).isRequired,
  source: shape({
    id: string.isRequired,
    links: shape({
      proxy: string.isRequired,
      self: string.isRequired,
    }),
  }),
  sources: array.isRequired,
}

export default withRouter(InfluxTable)
