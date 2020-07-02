// Libraries
import React, {FC, useEffect} from 'react'
import {connect} from 'react-redux'
import {Switch, Route} from 'react-router-dom'

// Components
import GetResource from 'src/resources/components/GetResource'
import GetResources from 'src/resources/components/GetResources'
import DashboardPage from 'src/dashboards/components/DashboardPage'
import GetTimeRange from 'src/dashboards/components/GetTimeRange'
import DashboardRoute from 'src/shared/components/DashboardRoute'
import EditVEO from 'src/dashboards/components/EditVEO'
import NewVEO from 'src/dashboards/components/NewVEO'

// // Overlays
// import OverlayHandler, {
//   RouteOverlay,
// } from 'src/overlays/components/RouteOverlay'
// const AddNoteOverlay = RouteOverlay(
//   OverlayHandler,
//   'add-note',
//   (history, params) => {
//     history.push(`/orgs/${params.orgID}/dashboards/${params.dashboardID}`)
//   }
// )
// const EditNoteOverlay = RouteOverlay(
//   OverlayHandler,
//   'edit-note',
//   (history, params) => {
//     history.push(`/orgs/${params.orgID}/dashboards/${params.dashboardID}`)
//   }
// )

// Actions
import {setCurrentPage} from 'src/shared/reducers/currentPage'

// Utils
import {GlobalAutoRefresher} from 'src/utils/AutoRefresher'

// Constants
import {AUTOREFRESH_DEFAULT} from 'src/shared/constants'

// Types
import {AppState, ResourceType, AutoRefresh, AutoRefreshStatus} from 'src/types'

const {Active} = AutoRefreshStatus

interface StateProps {
  autoRefresh: AutoRefresh
  dashboard: string
}

interface DispatchProps {
  onSetCurrentPage: typeof setCurrentPage
}

type Props = StateProps & DispatchProps

const dashRoute = '/orgs/:orgID/dashboards/:dashboardID'

const DashboardContainer: FC<Props> = ({
  autoRefresh,
  dashboard,
  onSetCurrentPage,
}) => {
  useEffect(() => {
    if (autoRefresh.status === Active) {
      GlobalAutoRefresher.poll(autoRefresh.interval)
      return
    }

    GlobalAutoRefresher.stopPolling()

    return function cleanup() {
      GlobalAutoRefresher.stopPolling()
    }
  }, [autoRefresh.status, autoRefresh.interval])

  useEffect(() => {
    onSetCurrentPage('dashboard')
    return () => {
      onSetCurrentPage('not set')
    }
  }, [])

  return (
    <DashboardRoute>
      <GetResource resources={[{type: ResourceType.Dashboards, id: dashboard}]}>
        <GetResources resources={[ResourceType.Buckets]}>
          <GetTimeRange />
          <DashboardPage autoRefresh={autoRefresh} />
          <Switch>
            <Route path={`${dashRoute}/cells`}>
              <Route path={`${dashRoute}/cells/new`} component={NewVEO} />
              <Route
                path={`${dashRoute}/cells/:cellID/edit`}
                component={EditVEO}
              />
              {/*<Route
                path={`${dashRoute}/notes/new`}
                component={AddNoteOverlay}
              />
              <Route
                path={`${dashRoute}/notes/:cellID/edit`}
                component={EditNoteOverlay}
              />*/}
            </Route>
          </Switch>
        </GetResources>
      </GetResource>
    </DashboardRoute>
  )
}

const mstp = (state: AppState): StateProps => {
  const dashboard = state.currentDashboard.id
  const autoRefresh = state.autoRefresh[dashboard] || AUTOREFRESH_DEFAULT
  return {
    autoRefresh,
    dashboard,
  }
}

const mdtp: DispatchProps = {
  onSetCurrentPage: setCurrentPage,
}

export default connect<StateProps, DispatchProps>(
  mstp,
  mdtp
)(DashboardContainer)
