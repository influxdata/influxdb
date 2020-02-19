// Libraries
import React, {FC, useEffect} from 'react'
import {connect} from 'react-redux'

// Components
import GetResource from 'src/resources/components/GetResource'
import DashboardPage from 'src/dashboards/components/DashboardPage'
import GetTimeRange from 'src/dashboards/components/GetTimeRange'
import DashboardRoute from 'src/shared/components/DashboardRoute'

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

type Props = StateProps

const DashboardContainer: FC<Props> = ({autoRefresh, dashboard, children}) => {
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
    <DashboardRoute>
      <GetResource resources={[{type: ResourceType.Dashboards, id: dashboard}]}>
        <GetTimeRange />
        <DashboardPage autoRefresh={autoRefresh} />
        {children}
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

export default connect<StateProps>(mstp)(DashboardContainer)
