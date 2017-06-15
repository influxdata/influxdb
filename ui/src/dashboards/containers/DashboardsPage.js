import React, {PropTypes} from 'react'
import {Link, withRouter} from 'react-router'
import {connect} from 'react-redux'
import {bindActionCreators} from 'redux'

import SourceIndicator from 'shared/components/SourceIndicator'
import DeleteConfirmTableCell from 'shared/components/DeleteConfirmTableCell'
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
        <div className="page-header">
          <div className="page-header__container">
            <div className="page-header__left">
              <h1 className="page-header__title">
                Dashboards
              </h1>
            </div>
            <div className="page-header__right">
              <SourceIndicator sourceName={this.props.source.name} />
            </div>
          </div>
        </div>
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
                    {dashboards && dashboards.length
                      ? <table className="table v-center admin-table table-highlight">
                          <thead>
                            <tr>
                              <th>Name</th>
                              <th />
                            </tr>
                          </thead>
                          <tbody>
                            {dashboards.map(dashboard =>
                              <tr key={dashboard.id} className="">
                                <td>
                                  <Link
                                    to={`${dashboardLink}/dashboards/${dashboard.id}`}
                                  >
                                    {dashboard.name}
                                  </Link>
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
                        </div>}
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
