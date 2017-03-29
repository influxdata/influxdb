import React, {PropTypes} from 'react'
import {Link, withRouter} from 'react-router'
import {connect} from 'react-redux'
import {bindActionCreators} from 'redux'

import SourceIndicator from 'shared/components/SourceIndicator'
import DeleteConfirmTableCell from 'shared/components/DeleteConfirmTableCell'

import {getDashboards, createDashboard} from '../apis'
import {deleteDashboardAsync} from 'src/dashboards/actions'

import {NEW_DASHBOARD} from 'src/dashboards/constants'

const {
  func,
  string,
  shape,
} = PropTypes

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
    addFlashMessage: func,
    handleDeleteDashboard: func.isRequired,
  },

  getInitialState() {
    return {
      dashboards: [],
      waiting: true,
    };
  },

  componentDidMount() {
    getDashboards().then((resp) => {
      this.setState({
        dashboards: resp.data.dashboards,
        waiting: false,
      });
    });
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
    const dashboardLink = `/sources/${this.props.source.id}`
    let tableHeader
    if (this.state.waiting) {
      tableHeader = "Loading Dashboards..."
    } else if (this.state.dashboards.length === 0) {
      tableHeader = "1 Dashboard"
    } else {
      tableHeader = `${this.state.dashboards.length + 1} Dashboards`
    }

    return (
      <div className="page">
        <div className="page-header">
          <div className="page-header__container">
            <div className="page-header__left">
              <h1>
                Dashboards
              </h1>
            </div>
            <div className="page-header__right">
              <SourceIndicator sourceName={this.props.source.name} />
            </div>
          </div>
        </div>
        <div className="page-contents">
          <div className="container-fluid">
            <div className="row">
              <div className="col-md-12">
                <div className="panel panel-minimal">
                  <div className="panel-heading u-flex u-ai-center u-jc-space-between">
                    <h2 className="panel-title">{tableHeader}</h2>
                    <button className="btn btn-sm btn-primary" onClick={this.handleCreateDashbord}>Create Dashboard</button>
                  </div>
                  <div className="panel-body">
                    <table className="table v-center">
                      <thead>
                        <tr>
                          <th>Name</th>
                        </tr>
                      </thead>
                      <tbody>
                          {
                            this.state.dashboards.map((dashboard) => {
                              return (
                                <tr key={dashboard.id} className="">
                                  <td className="monotype">
                                    <Link to={`${dashboardLink}/dashboards/${dashboard.id}`}>
                                      {dashboard.name}
                                    </Link>
                                  </td>
                                  <DeleteConfirmTableCell onDelete={this.handleDeleteDashboard} item={dashboard} />
                                </tr>
                              );
                            })
                          }
                          <tr>
                            <td className="monotype">
                              <Link to={`${dashboardLink}/kubernetes`}>
                                {'Kubernetes'}
                              </Link>
                            </td>
                          </tr>
                      </tbody>
                    </table>
                  </div>
                </div>
              </div>
            </div>
          </div>
        </div>
      </div>
    )
  },
})

const mapDispatchToProps = (dispatch) => ({
  handleDeleteDashboard: bindActionCreators(deleteDashboardAsync, dispatch),
})

export default connect(null, mapDispatchToProps)(withRouter(DashboardsPage))
