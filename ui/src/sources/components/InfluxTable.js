import React, {PropTypes} from 'react'
import {Link, withRouter} from 'react-router'
import {connect} from 'react-redux'

import Authorized, {EDITOR_ROLE} from 'src/auth/Authorized'

import Dropdown from 'shared/components/Dropdown'
import QuestionMarkTooltip from 'shared/components/QuestionMarkTooltip'

const kapacitorDropdown = (
  kapacitors,
  source,
  router,
  setActiveKapacitor,
  handleDeleteKapacitor
) => {
  if (!kapacitors || kapacitors.length === 0) {
    return (
      <Authorized requiredRole={EDITOR_ROLE}>
        <Link
          to={`/sources/${source.id}/kapacitors/new`}
          className="btn btn-xs btn-default"
        >
          <span className="icon plus" /> Add Config
        </Link>
      </Authorized>
    )
  }
  const kapacitorItems = kapacitors.map(k => {
    return {
      text: k.name,
      resource: `/sources/${source.id}/kapacitors/${k.id}`,
      kapacitor: k,
    }
  })

  const activeKapacitor = kapacitors.find(k => k.active)

  let selected = ''
  if (activeKapacitor) {
    selected = activeKapacitor.name
  } else {
    selected = kapacitorItems[0].text
  }

  const unauthorizedDropdown = (
    <div className="source-table--kapacitor__view-only">
      {selected}
    </div>
  )

  return (
    <Authorized
      requiredRole={EDITOR_ROLE}
      replaceWithIfNotAuthorized={unauthorizedDropdown}
    >
      <Dropdown
        className="dropdown-260"
        buttonColor="btn-primary"
        buttonSize="btn-xs"
        items={kapacitorItems}
        onChoose={setActiveKapacitor}
        addNew={{
          url: `/sources/${source.id}/kapacitors/new`,
          text: 'Add Kapacitor',
        }}
        actions={[
          {
            icon: 'pencil',
            text: 'edit',
            handler: item => {
              router.push(`${item.resource}/edit`)
            },
          },
          {
            icon: 'trash',
            text: 'delete',
            handler: item => {
              handleDeleteKapacitor(item.kapacitor)
            },
            confirmable: true,
          },
        ]}
        selected={selected}
      />
    </Authorized>
  )
}

const InfluxTable = ({
  source,
  router,
  sources,
  location,
  setActiveKapacitor,
  handleDeleteSource,
  handleDeleteKapacitor,
  isUsingAuth,
  me,
}) =>
  <div className="row">
    <div className="col-md-12">
      <div className="panel panel-minimal">
        <div className="panel-heading u-flex u-ai-center u-jc-space-between">
          <h2 className="panel-title">
            {isUsingAuth
              ? <span>
                  InfluxDB Connections for{' '}
                  <em>{me.currentOrganization.name}</em>
                </span>
              : <span>InfluxDB Connections</span>}
          </h2>
          <Authorized requiredRole={EDITOR_ROLE}>
            <Link
              to={`/sources/${source.id}/manage-sources/new`}
              className="btn btn-sm btn-primary"
            >
              <span className="icon plus" /> Add Connection
            </Link>
          </Authorized>
        </div>
        <div className="panel-body">
          <table className="table v-center margin-bottom-zero table-highlight">
            <thead>
              <tr>
                <th className="source-table--connect-col" />
                <th>Connection Name & Host</th>
                <th className="text-right" />
                <th>
                  Active Kapacitor{' '}
                  <QuestionMarkTooltip
                    tipID="kapacitor-node-helper"
                    tipContent={
                      '<p>Kapacitor Configurations are<br/>scoped per InfluxDB Connection.<br/>Only one can be active at a time.</p>'
                    }
                  />
                </th>
              </tr>
            </thead>
            <tbody>
              {sources.map(s => {
                return (
                  <tr
                    key={s.id}
                    className={s.id === source.id ? 'highlight' : null}
                  >
                    <td>
                      {s.id === source.id
                        ? <div className="btn btn-success btn-xs source-table--connect">
                            Connected
                          </div>
                        : <Link
                            className="btn btn-default btn-xs source-table--connect"
                            to={`/sources/${s.id}/hosts`}
                          >
                            Connect
                          </Link>}
                    </td>
                    <td>
                      <h5 className="margin-zero">
                        <Authorized
                          requiredRole={EDITOR_ROLE}
                          replaceWithIfNotAuthorized={
                            <strong>
                              {s.name}
                            </strong>
                          }
                        >
                          <Link
                            to={`${location.pathname}/${s.id}/edit`}
                            className={
                              s.id === source.id ? 'link-success' : null
                            }
                          >
                            <strong>
                              {s.name}
                            </strong>
                            {s.default ? ' (Default)' : null}
                          </Link>
                        </Authorized>
                      </h5>
                      <span>
                        {s.url}
                      </span>
                    </td>
                    <td className="text-right">
                      <Authorized requiredRole={EDITOR_ROLE}>
                        <a
                          className="btn btn-xs btn-danger table--show-on-row-hover"
                          href="#"
                          onClick={handleDeleteSource(s)}
                        >
                          Delete Connection
                        </a>
                      </Authorized>
                    </td>
                    <td className="source-table--kapacitor">
                      {kapacitorDropdown(
                        s.kapacitors,
                        s,
                        router,
                        setActiveKapacitor,
                        handleDeleteKapacitor
                      )}
                    </td>
                  </tr>
                )
              })}
            </tbody>
          </table>
        </div>
      </div>
    </div>
  </div>

const {array, bool, func, shape, string} = PropTypes

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
  setActiveKapacitor: func.isRequired,
  handleDeleteKapacitor: func.isRequired,
  me: shape({
    currentOrganization: shape({
      id: string.isRequired,
      name: string.isRequired,
    }),
  }),
  isUsingAuth: bool,
}

const mapStateToProps = ({auth: {isUsingAuth, me}}) => ({isUsingAuth, me})

export default connect(mapStateToProps)(withRouter(InfluxTable))
