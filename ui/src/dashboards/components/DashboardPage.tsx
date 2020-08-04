// Libraries
import React, {Component} from 'react'
import {connect, ConnectedProps} from 'react-redux'
import {Switch, Route} from 'react-router-dom'
import uuid from 'uuid'

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

// Utils
import {pageTitleSuffixer} from 'src/shared/utils/pageTitles'
import {event} from 'src/cloud/utils/reporting'
import {resetQueryCache} from 'src/shared/apis/queryCache'
import {isFlagEnabled} from 'src/shared/utils/featureFlag'

// Selectors & Actions
import {setRenderID as setRenderIDAction} from 'src/perf/actions'
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
    const {dashboard, setRenderID} = this.props
    const renderID = uuid.v4()
    setRenderID('dashboard', renderID)

    const tags = {
      dashboardID: dashboard.id,
    }
    const fields = {renderID}

    event('Dashboard Mounted', tags, fields)
    if (isFlagEnabled('queryCacheForDashboards')) {
      resetQueryCache()
    }
  }

  public componentDidUpdate(prevProps) {
    const {setRenderID, dashboard} = this.props

    if (prevProps.manualRefresh !== this.props.autoRefresh) {
      const renderID = uuid.v4()
      setRenderID('dashboard', renderID)
      const tags = {
        dashboardID: dashboard.id,
      }
      const fields = {renderID}

      event('Dashboard Mounted', tags, fields)
    }
  }

  public componentWillUnmount() {
    if (isFlagEnabled('queryCacheForDashboards')) {
      resetQueryCache()
    }
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
  setRenderID: setRenderIDAction,
}

const connector = connect(mstp, mdtp)

export default connector(ManualRefresh<OwnProps>(DashboardPage))
