// Libraries
import React, {Component} from 'react'
import {connect} from 'react-redux'

// Components
import {Page} from '@influxdata/clockface'
import {ErrorHandling} from 'src/shared/decorators/errors'
import DashboardHeader from 'src/dashboards/components/DashboardHeader'
import DashboardComponent from 'src/dashboards/components/Dashboard'
import ManualRefresh from 'src/shared/components/ManualRefresh'
import {HoverTimeProvider} from 'src/dashboards/utils/hoverTime'
import VariablesControlBar from 'src/dashboards/components/variablesControlBar/VariablesControlBar'
import LimitChecker from 'src/cloud/components/LimitChecker'
import RateLimitAlert from 'src/cloud/components/RateLimitAlert'

// Utils
import {pageTitleSuffixer} from 'src/shared/utils/pageTitles'

// Selectors
import {getByID} from 'src/resources/selectors'

// Types
import {AppState, AutoRefresh, ResourceType, Dashboard} from 'src/types'
import {ManualRefreshProps} from 'src/shared/components/ManualRefresh'

interface StateProps {
  dashboard: Dashboard
}

interface OwnProps {
  autoRefresh: AutoRefresh
}

type Props = OwnProps & StateProps & ManualRefreshProps

@ErrorHandling
class DashboardPage extends Component<Props> {
  public render() {
    const {autoRefresh, manualRefresh, onManualRefresh, children} = this.props

    return (
      <Page titleTag={this.pageTitle}>
        <LimitChecker>
          <HoverTimeProvider>
            <DashboardHeader
              autoRefresh={autoRefresh}
              onManualRefresh={onManualRefresh}
            />
            <RateLimitAlert className="dashboard--rate-alert" />
            <VariablesControlBar />
            <DashboardComponent manualRefresh={manualRefresh} />
            {children}
          </HoverTimeProvider>
        </LimitChecker>
      </Page>
    )
  }

  private get pageTitle(): string {
    const {dashboard} = this.props
    const title = dashboard && dashboard.name ? dashboard.name : 'Loading...'

    return pageTitleSuffixer([title])
  }
}

const mstp = (state: AppState): StateProps => {
  const dashboard = getByID<Dashboard>(
    state,
    ResourceType.Dashboards,
    state.currentDashboard.id
  )

  return {
    dashboard,
  }
}

export default connect<StateProps, {}>(
  mstp,
  null
)(ManualRefresh<OwnProps>(DashboardPage))
