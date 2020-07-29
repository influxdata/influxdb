// Libraries
import React, {Component} from 'react'
import {connect, ConnectedProps} from 'react-redux'
import {Switch, Route} from 'react-router-dom'

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
import EditVEO from 'src/dashboards/components/EditVEO'
import NewVEO from 'src/dashboards/components/NewVEO'
import {AddNoteOverlay, EditNoteOverlay} from 'src/overlays/components'
import {event} from 'src/cloud/utils/reporting'

// Utils
import {pageTitleSuffixer} from 'src/shared/utils/pageTitles'

// Selectors & Actions
import {resetCachedQueryResults} from 'src/queryCache/actions'
import {getByID} from 'src/resources/selectors'

// Types
import {AppState, AutoRefresh, ResourceType, Dashboard} from 'src/types'
import {ManualRefreshProps} from 'src/shared/components/ManualRefresh'

interface OwnProps {
  autoRefresh: AutoRefresh
}

type ReduxProps = ConnectedProps<typeof connector>
type Props = OwnProps & ManualRefreshProps & ReduxProps

import {
  ORGS,
  ORG_ID,
  DASHBOARDS,
  DASHBOARD_ID,
} from 'src/shared/constants/routes'

const dashRoute = `/${ORGS}/${ORG_ID}/${DASHBOARDS}/${DASHBOARD_ID}`

@ErrorHandling
class DashboardPage extends Component<Props> {
  public componentDidMount() {
    const {dashboard} = this.props
    event('Dashboard Mounted', {
      dashboardID: dashboard.id,
      orgID: dashboard.orgID,
    })
  }

  public componentWillUnmount() {
    this.props.resetCachedQueryResults()
  }

  public render() {
    const {autoRefresh, manualRefresh, onManualRefresh} = this.props

    return (
      <>
        <Page titleTag={this.pageTitle}>
          <LimitChecker>
            <HoverTimeProvider>
              <DashboardHeader
                autoRefresh={autoRefresh}
                onManualRefresh={onManualRefresh}
              />
              <RateLimitAlert alertOnly={true} />
              <VariablesControlBar />
              <DashboardComponent manualRefresh={manualRefresh} />
            </HoverTimeProvider>
          </LimitChecker>
        </Page>
        <Switch>
          <Route path={`${dashRoute}/cells/new`} component={NewVEO} />
          <Route path={`${dashRoute}/cells/:cellID/edit`} component={EditVEO} />
          <Route path={`${dashRoute}/notes/new`} component={AddNoteOverlay} />
          <Route
            path={`${dashRoute}/notes/:cellID/edit`}
            component={EditNoteOverlay}
          />
        </Switch>
      </>
    )
  }

  private get pageTitle(): string {
    const {dashboard} = this.props
    const title = dashboard && dashboard.name ? dashboard.name : 'Loading...'

    return pageTitleSuffixer([title])
  }
}

const mstp = (state: AppState) => {
  const dashboard = getByID<Dashboard>(
    state,
    ResourceType.Dashboards,
    state.currentDashboard.id
  )

  return {
    dashboard,
  }
}

const mdtp = {
  resetCachedQueryResults: resetCachedQueryResults,
}

const connector = connect(mstp, mdtp)

export default connector(ManualRefresh<OwnProps>(DashboardPage))
