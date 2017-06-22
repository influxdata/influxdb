import React, {PropTypes} from 'react'
import {withRouter} from 'react-router'
import {connect} from 'react-redux'
import {bindActionCreators} from 'redux'

import DashboardsHeader from 'src/dashboards/components/DashboardsHeader'
import DashboardsTable from 'src/dashboards/components/DashboardsTable'
import FancyScrollbar from 'shared/components/FancyScrollbar'

import {createDashboard} from 'src/dashboards/apis'
import {getDashboardsAsync, deleteDashboardAsync} from 'src/dashboards/actions'

import {NEW_DASHBOARD} from 'src/dashboards/constants'

const {arrayOf, func, string, shape} = PropTypes

const DashboardsPage = React.createClass({
  propTypes: {
    source: shape({
      id: string.isRequired,
      name: string.isRequired,
      type: string,
      links: shape({
        proxy: string.isRequired,
      }).isRequired,
      telegraf: string.isRequired,
    }),
    router: shape({
      push: func.isRequired,
    }).isRequired,
    handleGetDashboards: func.isRequired,
    handleDeleteDashboard: func.isRequired,
    dashboards: arrayOf(shape()),
  },

  componentDidMount() {
    this.props.handleGetDashboards()
  },

  async handleCreateDashbord() {
    const {source: {id}, router: {push}} = this.props
    const {data} = await createDashboard(NEW_DASHBOARD)
    push(`/sources/${id}/dashboards/${data.id}`)
  },

  handleDeleteDashboard(dashboard) {
    this.props.handleDeleteDashboard(dashboard)
  },

  render() {
    const {dashboards} = this.props
    const dashboardLink = `/sources/${this.props.source.id}`
    let tableHeader
    if (dashboards === null) {
      tableHeader = 'Loading Dashboards...'
    } else if (dashboards.length === 1) {
      tableHeader = '1 Dashboard'
    } else {
      tableHeader = `${dashboards.length} Dashboards`
    }

    return (
      <div className="page">
        <DashboardsHeader sourceName={this.props.source.name} />
        <FancyScrollbar className="page-contents">
          <div className="container-fluid">
            <div className="row">
              <div className="col-md-12">
                <div className="panel panel-minimal">
                  <div className="panel-heading u-flex u-ai-center u-jc-space-between">
                    <h2 className="panel-title">{tableHeader}</h2>
                    <button
                      className="btn btn-sm btn-primary"
                      onClick={this.handleCreateDashbord}
                    >
                      Create Dashboard
                    </button>
                  </div>
                  <div className="panel-body">
                    <DashboardsTable
                      dashboards={dashboards}
                      onDeleteDashboard={this.handleDeleteDashboard}
                      onCreateDashboard={this.handleCreateDashbord}
                      dashboardLink={dashboardLink}
                    />
                    {/* {dashboards && dashboards.length
                      ? <table className="table v-center admin-table table-highlight">
                          <thead>
                            <tr>
                              <th>Name</th>
                              <th>Template Variables</th>
                              <th />
                            </tr>
                          </thead>
                          <tbody>
                            {_.sortBy(dashboards, d =>
                              d.name.toLowerCase()
                            ).map(dashboard =>
                              <tr key={dashboard.id}>
                                <td>
                                  <Link
                                    to={`${dashboardLink}/dashboards/${dashboard.id}`}
                                  >
                                    {dashboard.name}
                                  </Link>
                                </td>
                                <td>
                                  {dashboard.templates.length
                                    ? dashboard.templates.map(tv =>
                                        <code
                                          className="table--temp-var"
                                          key={tv.id}
                                        >
                                          {tv.tempVar}
                                        </code>
                                      )
                                    : <span className="empty-string">
                                        None
                                      </span>}
                                </td>
                                <DeleteConfirmTableCell
                                  onDelete={this.handleDeleteDashboard}
                                  item={dashboard}
                                  buttonSize="btn-xs"
                                />
                              </tr>
                            )}
                          </tbody>
                        </table>
                      : <div className="generic-empty-state">
                          <h4 style={{marginTop: '90px'}}>
                            Looks like you dont have any dashboards
                          </h4>
                          <button
                            className="btn btn-sm btn-primary"
                            onClick={this.handleCreateDashbord}
                            style={{marginBottom: '90px'}}
                          >
                            Create Dashboard
                          </button>
                        </div>} */}
                  </div>
                </div>
              </div>
            </div>
          </div>
        </FancyScrollbar>
      </div>
    )
  },
})

const mapStateToProps = ({dashboardUI: {dashboards, dashboard}}) => ({
  dashboards,
  dashboard,
})

const mapDispatchToProps = dispatch => ({
  handleGetDashboards: bindActionCreators(getDashboardsAsync, dispatch),
  handleDeleteDashboard: bindActionCreators(deleteDashboardAsync, dispatch),
})

export default connect(mapStateToProps, mapDispatchToProps)(
  withRouter(DashboardsPage)
)
