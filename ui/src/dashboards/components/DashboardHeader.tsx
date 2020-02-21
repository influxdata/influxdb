// Libraries
import React, {Component} from 'react'
import {connect} from 'react-redux'
import {withRouter, WithRouterProps} from 'react-router'

// Components
import AutoRefreshDropdown from 'src/shared/components/dropdown_auto_refresh/AutoRefreshDropdown'
import TimeRangeDropdown from 'src/shared/components/TimeRangeDropdown'
import GraphTips from 'src/shared/components/graph_tips/GraphTips'
import RenamablePageTitle from 'src/pageLayout/components/RenamablePageTitle'
import TimeZoneDropdown from 'src/shared/components/TimeZoneDropdown'
import {
  SquareButton,
  Button,
  IconFont,
  ComponentColor,
  Page,
} from '@influxdata/clockface'

// Actions
import {delayEnablePresentationMode} from 'src/shared/actions/app'
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
import {getTimeRangeByDashboardID} from 'src/dashboards/selectors'
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
  handleClickPresentationButton: typeof delayEnablePresentationMode
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
      org,
      dashboard,
      onManualRefresh,
      toggleShowVariablesControls,
      showVariablesControls,
      autoRefresh,
      timeRange,
    } = this.props

    return (
      <Page.Header fullWidth={true}>
        <Page.HeaderLeft>
          <RenamablePageTitle
            prefix={org && org.name}
            maxLength={DASHBOARD_NAME_MAX_LENGTH}
            onRename={this.handleRenameDashboard}
            name={dashboard && dashboard.name}
            placeholder={DEFAULT_DASHBOARD_NAME}
          />
        </Page.HeaderLeft>
        <Page.HeaderRight>
          <GraphTips />
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
          <SquareButton
            icon={IconFont.ExpandA}
            titleText="Enter Presentation Mode"
            onClick={this.handleClickPresentationButton}
          />
        </Page.HeaderRight>
      </Page.Header>
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

  private handleClickPresentationButton = (): void => {
    this.props.handleClickPresentationButton()
  }

  private handleChooseAutoRefresh = (milliseconds: number) => {
    const {handleChooseAutoRefresh, dashboard} = this.props
    handleChooseAutoRefresh(dashboard.id, milliseconds)
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

  const timeRange = getTimeRangeByDashboardID(state, state.currentDashboard.id)
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
  handleClickPresentationButton: delayEnablePresentationMode,
  onSetAutoRefreshStatus: setAutoRefreshStatus,
  updateQueryParams: updateQueryParams,
  setDashboardTimeRange: setDashboardTimeRange,
  handleChooseAutoRefresh: setAutoRefreshInterval,
}

export default connect<StateProps, DispatchProps, OwnProps>(
  mstp,
  mdtp
)(withRouter<OwnProps>(DashboardHeader))
