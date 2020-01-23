// Libraries
import React, {FC, useEffect} from 'react'
import {withRouter, WithRouterProps} from 'react-router'
import {connect} from 'react-redux'

// Components
import GetResource from 'src/resources/components/GetResource'
import DashboardPage from 'src/dashboards/components/DashboardPage'
import GetTimeRange from 'src/dashboards/components/GetTimeRange'

// Utils
import {GlobalAutoRefresher} from 'src/utils/AutoRefresher'

// Constants
import {AUTOREFRESH_DEFAULT} from 'src/shared/constants'

// Types
import {AppState, ResourceType, AutoRefresh, AutoRefreshStatus} from 'src/types'

const {Active} = AutoRefreshStatus
interface StateProps {
  autoRefresh: AutoRefresh
}

type Props = WithRouterProps & StateProps

const DashboardContainer: FC<Props> = ({autoRefresh, params, children}) => {
  const {dashboardID, orgID} = params
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

  return (
    <GetResource resources={[{type: ResourceType.Dashboards, id: dashboardID}]}>
      <GetTimeRange />
      <DashboardPage
        orgID={orgID}
        dashboardID={dashboardID}
        autoRefresh={autoRefresh}
      />
      {children}
    </GetResource>
  )
}

const mstp = (state: AppState, {params}: Props): StateProps => {
  const autoRefresh =
    state.autoRefresh[params.dashboardID] || AUTOREFRESH_DEFAULT
  return {autoRefresh}
}

export default withRouter(connect<StateProps>(mstp)(DashboardContainer))
