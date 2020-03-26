// Libraries
import React, {Component} from 'react'
import {connect} from 'react-redux'
import {withRouter, WithRouterProps} from 'react-router'

// Components
import AutoRefreshDropdown from 'src/shared/components/dropdown_auto_refresh/AutoRefreshDropdown'
import TimeRangeDropdown from 'src/shared/components/TimeRangeDropdown'
import PresentationModeToggle from 'src/shared/components/PresentationModeToggle'
import DashboardLightModeToggle from 'src/dashboards/components/DashboardLightModeToggle'
import GraphTips from 'src/shared/components/graph_tips/GraphTips'
import RenamablePageTitle from 'src/pageLayout/components/RenamablePageTitle'
import TimeZoneDropdown from 'src/shared/components/TimeZoneDropdown'
import {Button, IconFont, ComponentColor, Page} from '@influxdata/clockface'

// Actions
import {toggleShowVariablesControls} from 'src/userSettings/actions'
import {updateDashboard} from 'src/dashboards/actions/thunks'
import {
  setAutoRefreshInterval,
  setAutoRefreshStatus,
} from 'src/shared/actions/autoRefresh'
import {
  setDashboardTimeRange,
  updateQueryParams,
} from 'src/dashboards/actions/ranges'

// Selectors
import {getTimeRange} from 'src/dashboards/selectors'
import {getByID} from 'src/resources/selectors'
import {getOrg} from 'src/organizations/selectors'

// Constants
import {
  DEFAULT_DASHBOARD_NAME,
  DASHBOARD_NAME_MAX_LENGTH,
} from 'src/dashboards/constants/index'

// Types
import {
  AppState,
  AutoRefresh,
  AutoRefreshStatus,
  Dashboard,
  Organization,
  ResourceType,
  TimeRange,
} from 'src/types'

interface OwnProps {
  autoRefresh: AutoRefresh
  onManualRefresh: () => void
}

interface StateProps {
  org: Organization
  dashboard: Dashboard
  showVariablesControls: boolean
  timeRange: TimeRange
}

interface DispatchProps {
  toggleShowVariablesControls: typeof toggleShowVariablesControls
  updateDashboard: typeof updateDashboard
  onSetAutoRefreshStatus: typeof setAutoRefreshStatus
  updateQueryParams: typeof updateQueryParams
  setDashboardTimeRange: typeof setDashboardTimeRange
  handleChooseAutoRefresh: typeof setAutoRefreshInterval
}

type Props = OwnProps & StateProps & DispatchProps & WithRouterProps

class DashboardHeader extends Component<Props> {
  public render() {
    const {
      dashboard,
      onManualRefresh,
      toggleShowVariablesControls,
      showVariablesControls,
      autoRefresh,
      timeRange,
    } = this.props

    return (
      <>
        <Page.Header fullWidth={true}>
          <RenamablePageTitle
            maxLength={DASHBOARD_NAME_MAX_LENGTH}
            onRename={this.handleRenameDashboard}
            name={dashboard && dashboard.name}
            placeholder={DEFAULT_DASHBOARD_NAME}
          />
        </Page.Header>
        <Page.ControlBar fullWidth={true}>
          <Page.ControlBarLeft>
            <Button
              icon={IconFont.AddCell}
              color={ComponentColor.Primary}
              onClick={this.handleAddCell}
              text="Add Cell"
              titleText="Add cell to dashboard"
            />
            <Button
              icon={IconFont.TextBlock}
              text="Add Note"
              onClick={this.handleAddNote}
            />
            <Button
              icon={IconFont.Cube}
              text="Variables"
              onClick={toggleShowVariablesControls}
              color={
                showVariablesControls
                  ? ComponentColor.Secondary
                  : ComponentColor.Default
              }
            />
            <DashboardLightModeToggle />
            <PresentationModeToggle />
            <GraphTips />
          </Page.ControlBarLeft>
          <Page.ControlBarRight>
            <TimeZoneDropdown />
            <AutoRefreshDropdown
              onChoose={this.handleChooseAutoRefresh}
              onManualRefresh={onManualRefresh}
              selected={autoRefresh}
            />
            <TimeRangeDropdown
              onSetTimeRange={this.handleChooseTimeRange}
              timeRange={timeRange}
            />
          </Page.ControlBarRight>
        </Page.ControlBar>
      </>
    )
  }

  private handleAddNote = () => {
    const {router, org, dashboard} = this.props
    router.push(`/orgs/${org.id}/dashboards/${dashboard.id}/notes/new`)
  }

  private handleAddCell = () => {
    const {router, org, dashboard} = this.props
    router.push(`/orgs/${org.id}/dashboards/${dashboard.id}/cells/new`)
  }

  private handleRenameDashboard = (name: string) => {
    const {dashboard, updateDashboard} = this.props

    updateDashboard(dashboard.id, {name})
  }

  private handleChooseAutoRefresh = (milliseconds: number) => {
    const {
      handleChooseAutoRefresh,
      onSetAutoRefreshStatus,
      dashboard,
    } = this.props
    handleChooseAutoRefresh(dashboard.id, milliseconds)

    if (milliseconds === 0) {
      onSetAutoRefreshStatus(dashboard.id, AutoRefreshStatus.Paused)
      return
    }

    onSetAutoRefreshStatus(dashboard.id, AutoRefreshStatus.Active)
  }

  private handleChooseTimeRange = (timeRange: TimeRange) => {
    const {
      autoRefresh,
      onSetAutoRefreshStatus,
      setDashboardTimeRange,
      updateQueryParams,
      dashboard,
    } = this.props

    setDashboardTimeRange(dashboard.id, timeRange)
    updateQueryParams({
      lower: timeRange.lower,
      upper: timeRange.upper,
    })

    if (timeRange.type === 'custom') {
      onSetAutoRefreshStatus(dashboard.id, AutoRefreshStatus.Disabled)
      return
    }

    if (autoRefresh.status === AutoRefreshStatus.Disabled) {
      if (autoRefresh.interval === 0) {
        onSetAutoRefreshStatus(dashboard.id, AutoRefreshStatus.Paused)
        return
      }

      onSetAutoRefreshStatus(dashboard.id, AutoRefreshStatus.Active)
    }
  }
}

const mstp = (state: AppState): StateProps => {
  const {showVariablesControls} = state.userSettings
  const dashboard = getByID<Dashboard>(
    state,
    ResourceType.Dashboards,
    state.currentDashboard.id
  )

  const timeRange = getTimeRange(state, state.currentDashboard.id)
  const org = getOrg(state)

  return {
    org,
    dashboard,
    timeRange,
    showVariablesControls,
  }
}

const mdtp: DispatchProps = {
  toggleShowVariablesControls: toggleShowVariablesControls,
  updateDashboard: updateDashboard,
  onSetAutoRefreshStatus: setAutoRefreshStatus,
  updateQueryParams: updateQueryParams,
  setDashboardTimeRange: setDashboardTimeRange,
  handleChooseAutoRefresh: setAutoRefreshInterval,
}

export default connect<StateProps, DispatchProps, OwnProps>(
  mstp,
  mdtp
)(withRouter<OwnProps>(DashboardHeader))
