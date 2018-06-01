import React, {Component} from 'react'
import PropTypes from 'prop-types'
import {withRouter} from 'react-router'
import {connect} from 'react-redux'
import {bindActionCreators} from 'redux'

import DashboardsHeader from 'src/dashboards/components/DashboardsHeader'
import DashboardsContents from 'src/dashboards/components/DashboardsPageContents'

import {createDashboard} from 'src/dashboards/apis'
import {
  getDashboardsAsync,
  deleteDashboardAsync,
  pruneDashTimeV1 as pruneDashTimeV1Action,
} from 'src/dashboards/actions'

import {NEW_DASHBOARD} from 'src/dashboards/constants'
import {ErrorHandling} from 'src/shared/decorators/errors'

@ErrorHandling
class DashboardsPage extends Component {
  async componentDidMount() {
    const dashboards = await this.props.handleGetDashboards()
    const dashboardIDs = dashboards.map(d => d.id)
    this.props.pruneDashTimeV1(dashboardIDs)
  }

  handleCreateDashboard = async () => {
    const {
      source: {id},
      router: {push},
    } = this.props
    const {data} = await createDashboard(NEW_DASHBOARD)
    push(`/sources/${id}/dashboards/${data.id}`)
  }

  handleCloneDashboard = dashboard => async () => {
    const {
      source: {id},
      router: {push},
    } = this.props
    const {data} = await createDashboard({
      ...dashboard,
      name: `${dashboard.name} (clone)`,
    })
    push(`/sources/${id}/dashboards/${data.id}`)
  }

  handleDeleteDashboard = dashboard => () => {
    this.props.handleDeleteDashboard(dashboard)
  }

  render() {
    const {dashboards} = this.props
    const dashboardLink = `/sources/${this.props.source.id}`

    return (
      <div className="page">
        <DashboardsHeader sourceName={this.props.source.name} />
        <DashboardsContents
          dashboardLink={dashboardLink}
          dashboards={dashboards}
          onDeleteDashboard={this.handleDeleteDashboard}
          onCreateDashboard={this.handleCreateDashboard}
          onCloneDashboard={this.handleCloneDashboard}
        />
      </div>
    )
  }
}

const {arrayOf, func, string, shape} = PropTypes

DashboardsPage.propTypes = {
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
  pruneDashTimeV1: func.isRequired,
  dashboards: arrayOf(shape()),
}

const mapStateToProps = ({dashboardUI: {dashboards, dashboard}}) => ({
  dashboards,
  dashboard,
})

const mapDispatchToProps = dispatch => ({
  handleGetDashboards: bindActionCreators(getDashboardsAsync, dispatch),
  handleDeleteDashboard: bindActionCreators(deleteDashboardAsync, dispatch),
  pruneDashTimeV1: bindActionCreators(pruneDashTimeV1Action, dispatch),
})

export default connect(mapStateToProps, mapDispatchToProps)(
  withRouter(DashboardsPage)
)
