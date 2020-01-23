// Libraries
import React, {Component} from 'react'
import {connect} from 'react-redux'
import {withRouter} from 'react-router'

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

// Actions
import * as cellActions from 'src/cells/actions/thunks'
import * as dashboardActions from 'src/dashboards/actions/thunks'
import * as rangesActions from 'src/dashboards/actions/ranges'
import * as appActions from 'src/shared/actions/app'
import {updateViewAndVariables} from 'src/views/actions/thunks'
import {
  setAutoRefreshInterval,
  setAutoRefreshStatus,
} from 'src/shared/actions/autoRefresh'
import {toggleShowVariablesControls} from 'src/userSettings/actions'

// Utils
import {
  extractRateLimitResources,
  extractRateLimitStatus,
} from 'src/cloud/utils/limits'
import {pageTitleSuffixer} from 'src/shared/utils/pageTitles'

// Selectors
import {getTimeRangeByDashboardID} from 'src/dashboards/selectors'
import {getByID} from 'src/resources/selectors'
import {getOrg} from 'src/organizations/selectors'

// Types
import {
  Links,
  Cell,
  TimeRange,
  AppState,
  AutoRefresh,
  AutoRefreshStatus,
  ResourceType,
  Dashboard,
} from 'src/types'
import {WithRouterProps} from 'react-router'
import {ManualRefreshProps} from 'src/shared/components/ManualRefresh'
import * as AppActions from 'src/types/actions/app'
import {LimitStatus} from 'src/cloud/actions/limits'

interface StateProps {
  orgName: string
  dashboardName: string
  limitedResources: string[]
  limitStatus: LimitStatus
  links: Links
  timeRange: TimeRange
  showVariablesControls: boolean
}

interface DispatchProps {
  deleteCell: typeof cellActions.deleteCell
  getDashboard: typeof dashboardActions.getDashboard
  updateDashboard: typeof dashboardActions.updateDashboard
  updateCells: typeof cellActions.updateCells
  updateQueryParams: typeof rangesActions.updateQueryParams
  setDashboardTimeRange: typeof rangesActions.setDashboardTimeRange
  handleChooseAutoRefresh: typeof setAutoRefreshInterval
  onSetAutoRefreshStatus: typeof setAutoRefreshStatus
  handleClickPresentationButton: AppActions.DelayEnablePresentationModeDispatcher
  onUpdateView: typeof updateViewAndVariables
  onToggleShowVariablesControls: typeof toggleShowVariablesControls
}

interface OwnProps {
  autoRefresh: AutoRefresh
  dashboardID: string
  orgID: string
}

type Props = OwnProps &
  StateProps &
  DispatchProps &
  ManualRefreshProps &
  WithRouterProps

@ErrorHandling
class DashboardPage extends Component<Props> {
  public render() {
    const {
      orgName,
      dashboardName,
      params,
      timeRange,
      autoRefresh,
      limitStatus,
      limitedResources,
      manualRefresh,
      onManualRefresh,
      showVariablesControls,
      handleClickPresentationButton,
      onToggleShowVariablesControls,
      children,
    } = this.props

    const {dashboardID} = params

    return (
      <Page titleTag={this.pageTitle}>
        <LimitChecker>
          <HoverTimeProvider>
            <DashboardHeader
              orgName={orgName}
              dashboardName={dashboardName}
              timeRange={timeRange}
              autoRefresh={autoRefresh}
              onAddNote={this.handleAddNote}
              onAddCell={this.handleAddCell}
              onManualRefresh={onManualRefresh}
              onRenameDashboard={this.handleRenameDashboard}
              handleChooseAutoRefresh={this.handleChooseAutoRefresh}
              onSetAutoRefreshStatus={this.handleSetAutoRefreshStatus}
              handleChooseTimeRange={this.handleChooseTimeRange}
              handleClickPresentationButton={handleClickPresentationButton}
              toggleVariablesControlBar={onToggleShowVariablesControls}
              isShowingVariablesControlBar={showVariablesControls}
            />
            <RateLimitAlert
              resources={limitedResources}
              limitStatus={limitStatus}
              className="dashboard--rate-alert"
            />
            {showVariablesControls && (
              <VariablesControlBar dashboardID={dashboardID} />
            )}
            <DashboardComponent
              timeRange={timeRange}
              dashboardID={dashboardID}
              manualRefresh={manualRefresh}
              onAddCell={this.handleAddCell}
              onPositionChange={this.handlePositionChange}
            />
            {children}
          </HoverTimeProvider>
        </LimitChecker>
      </Page>
    )
  }

  private handleAddNote = () => {
    const {router, location} = this.props
    router.push(`${location.pathname}/notes/new`)
  }

  private handleChooseTimeRange = (timeRange: TimeRange): void => {
    const {setDashboardTimeRange, updateQueryParams, dashboardID} = this.props
    setDashboardTimeRange(dashboardID, timeRange)
    updateQueryParams({
      lower: timeRange.lower,
      upper: timeRange.upper,
    })
  }

  private handleSetAutoRefreshStatus = (
    autoRefreshStatus: AutoRefreshStatus
  ) => {
    const {onSetAutoRefreshStatus, dashboardID} = this.props

    onSetAutoRefreshStatus(dashboardID, autoRefreshStatus)
  }

  private handleChooseAutoRefresh = (autoRefreshInterval: number) => {
    const {
      handleChooseAutoRefresh,
      params: {dashboardID},
    } = this.props

    handleChooseAutoRefresh(dashboardID, autoRefreshInterval)

    if (autoRefreshInterval === 0) {
      this.handleSetAutoRefreshStatus(AutoRefreshStatus.Paused)
      return
    }

    this.handleSetAutoRefreshStatus(AutoRefreshStatus.Active)
  }

  private handlePositionChange = (cells: Cell[]) => {
    const {dashboardID, updateCells} = this.props
    updateCells(dashboardID, cells)
  }

  private handleAddCell = () => {
    const {router, location} = this.props
    router.push(`${location.pathname}/cells/new`)
  }

  private handleRenameDashboard = (name: string) => {
    const {params, updateDashboard} = this.props

    updateDashboard(params.dashboardID, {name})
  }

  private get pageTitle(): string {
    const {dashboardName} = this.props
    const title = dashboardName ? dashboardName : 'Loading...'

    return pageTitleSuffixer([title])
  }
}

const mstp = (state: AppState, {dashboardID}: OwnProps): StateProps => {
  const {
    links,
    userSettings: {showVariablesControls},
    cloud: {limits},
  } = state

  const dashboard = getByID<Dashboard>(
    state,
    ResourceType.Dashboards,
    dashboardID
  )

  const timeRange = getTimeRangeByDashboardID(state, dashboardID)
  const limitedResources = extractRateLimitResources(limits)
  const limitStatus = extractRateLimitStatus(limits)
  const org = getOrg(state)

  return {
    links,
    orgName: org && org.name,
    timeRange,
    dashboardName: dashboard && dashboard.name,
    limitStatus,
    limitedResources,
    showVariablesControls,
  }
}

const mdtp: DispatchProps = {
  getDashboard: dashboardActions.getDashboard,
  updateDashboard: dashboardActions.updateDashboard,
  handleChooseAutoRefresh: setAutoRefreshInterval,
  onSetAutoRefreshStatus: setAutoRefreshStatus,
  handleClickPresentationButton: appActions.delayEnablePresentationMode,
  setDashboardTimeRange: rangesActions.setDashboardTimeRange,
  updateQueryParams: rangesActions.updateQueryParams,
  updateCells: cellActions.updateCells,
  deleteCell: cellActions.deleteCell,
  onUpdateView: updateViewAndVariables,
  onToggleShowVariablesControls: toggleShowVariablesControls,
}

export default connect<StateProps, DispatchProps>(
  mstp,
  mdtp
)(
  ManualRefresh<OwnProps>(
    withRouter<OwnProps & ManualRefreshProps>(DashboardPage)
  )
)
