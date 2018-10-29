// Libraries
import React, {Component} from 'react'

// Components
import {Page} from 'src/pageLayout'
import AutoRefreshDropdown from 'src/shared/components/dropdown_auto_refresh/AutoRefreshDropdown'
import TimeRangeDropdown from 'src/shared/components/TimeRangeDropdown'
import GraphTips from 'src/shared/components/graph_tips/GraphTips'
import RenameDashboard from 'src/dashboards/components/rename_dashboard/RenameDashboard'
import {Button, ButtonShape, ComponentColor, IconFont} from 'src/clockface'

// Types
import * as AppActions from 'src/types/actions/app'
import * as QueriesModels from 'src/types/queries'
import {Dashboard, DashboardSwitcherLinks} from 'src/types/v2/dashboards'

interface Props {
  activeDashboard: string
  dashboard: Dashboard
  timeRange: QueriesModels.TimeRange
  autoRefresh: number
  handleChooseTimeRange: (timeRange: QueriesModels.TimeRange) => void
  handleChooseAutoRefresh: AppActions.SetAutoRefreshActionCreator
  onManualRefresh: () => void
  handleClickPresentationButton: AppActions.DelayEnablePresentationModeDispatcher
  onAddCell: () => void
  showTemplateControlBar: boolean
  zoomedTimeRange: QueriesModels.TimeRange
  onRenameDashboard: (name: string) => Promise<void>
  dashboardLinks: DashboardSwitcherLinks
  isHidden: boolean
}

class DashboardHeader extends Component<Props> {
  public static defaultProps: Partial<Props> = {
    zoomedTimeRange: {
      upper: null,
      lower: null,
    },
  }

  public render() {
    const {
      handleChooseAutoRefresh,
      onManualRefresh,
      autoRefresh,
      handleChooseTimeRange,
      timeRange: {upper, lower},
      zoomedTimeRange: {upper: zoomedUpper, lower: zoomedLower},
      isHidden,
    } = this.props

    return (
      <Page.Header fullWidth={true} inPresentationMode={isHidden}>
        <Page.Header.Left>{this.dashboardTitle}</Page.Header.Left>
        <Page.Header.Right>
          <GraphTips />
          {this.addCellButton}
          <AutoRefreshDropdown
            onChoose={handleChooseAutoRefresh}
            onManualRefresh={onManualRefresh}
            selected={autoRefresh}
          />
          <TimeRangeDropdown
            onSetTimeRange={handleChooseTimeRange}
            timeRange={{
              upper: zoomedUpper || upper,
              lower: zoomedLower || lower,
            }}
          />
          <Button
            icon={IconFont.ExpandA}
            titleText="Enter Presentation Mode"
            shape={ButtonShape.Square}
            onClick={this.handleClickPresentationButton}
          />
        </Page.Header.Right>
      </Page.Header>
    )
  }

  private handleClickPresentationButton = (): void => {
    this.props.handleClickPresentationButton()
  }

  private get addCellButton(): JSX.Element {
    const {dashboard, onAddCell} = this.props

    if (dashboard) {
      return (
        <Button
          shape={ButtonShape.Square}
          icon={IconFont.AddCell}
          color={ComponentColor.Primary}
          onClick={onAddCell}
          titleText="Add cell to dashboard"
        />
      )
    }
  }

  private get dashboardTitle(): JSX.Element {
    const {dashboard, activeDashboard, onRenameDashboard} = this.props

    if (dashboard) {
      return (
        <RenameDashboard onRename={onRenameDashboard} name={activeDashboard} />
      )
    }

    return <Page.Title title={activeDashboard} />
  }
}

export default DashboardHeader
