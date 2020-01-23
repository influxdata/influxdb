// Libraries
import React, {Component} from 'react'

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

// Constants
import {
  DEFAULT_DASHBOARD_NAME,
  DASHBOARD_NAME_MAX_LENGTH,
} from 'src/dashboards/constants/index'

// Types
import * as AppActions from 'src/types/actions/app'
import {AutoRefresh, AutoRefreshStatus, TimeRange} from 'src/types'

interface Props {
  orgName: string
  dashboardName: string
  timeRange: TimeRange
  autoRefresh: AutoRefresh
  handleChooseTimeRange: (timeRange: TimeRange) => void
  handleChooseAutoRefresh: (autoRefreshInterval: number) => void
  onSetAutoRefreshStatus: (status: AutoRefreshStatus) => void
  onManualRefresh: () => void
  handleClickPresentationButton: AppActions.DelayEnablePresentationModeDispatcher
  onAddCell: () => void
  onAddNote: () => void
  onRenameDashboard: (name: string) => void
  toggleVariablesControlBar: () => void
  isShowingVariablesControlBar: boolean
}

export default class DashboardHeader extends Component<Props> {
  public render() {
    const {
      orgName,
      handleChooseAutoRefresh,
      onManualRefresh,
      toggleVariablesControlBar,
      isShowingVariablesControlBar,
      onRenameDashboard,
      onAddCell,
      dashboardName,
      autoRefresh,
      timeRange,
    } = this.props

    return (
      <Page.Header fullWidth={true}>
        <Page.HeaderLeft>
          <RenamablePageTitle
            prefix={orgName}
            maxLength={DASHBOARD_NAME_MAX_LENGTH}
            onRename={onRenameDashboard}
            name={dashboardName}
            placeholder={DEFAULT_DASHBOARD_NAME}
          />
        </Page.HeaderLeft>
        <Page.HeaderRight>
          <GraphTips />
          <Button
            icon={IconFont.AddCell}
            color={ComponentColor.Primary}
            onClick={onAddCell}
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
            onChoose={handleChooseAutoRefresh}
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
            onClick={toggleVariablesControlBar}
            color={
              isShowingVariablesControlBar
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
    this.props.onAddNote()
  }

  private handleClickPresentationButton = (): void => {
    this.props.handleClickPresentationButton()
  }

  private handleChooseTimeRange = (timeRange: TimeRange) => {
    const {
      autoRefresh,
      onSetAutoRefreshStatus,
      handleChooseTimeRange,
    } = this.props

    handleChooseTimeRange(timeRange)

    if (timeRange.type === 'custom') {
      onSetAutoRefreshStatus(AutoRefreshStatus.Disabled)
      return
    }

    if (autoRefresh.status === AutoRefreshStatus.Disabled) {
      if (autoRefresh.interval === 0) {
        onSetAutoRefreshStatus(AutoRefreshStatus.Paused)
        return
      }

      onSetAutoRefreshStatus(AutoRefreshStatus.Active)
    }
  }
}
