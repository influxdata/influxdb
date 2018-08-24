import React, {Component} from 'react'

import PageHeader from 'src/reusable_ui/components/page_layout/PageHeader'
import PageHeaderTitle from 'src/reusable_ui/components/page_layout/PageHeaderTitle'
import AutoRefreshDropdown from 'src/shared/components/AutoRefreshDropdown'
import TimeRangeDropdown from 'src/shared/components/TimeRangeDropdown'
import GraphTips from 'src/shared/components/GraphTips'
import RenameDashboard from 'src/dashboards/components/rename_dashboard/RenameDashboard'
import DashboardSwitcher from 'src/dashboards/components/DashboardSwitcher'

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
    const {isHidden} = this.props

    return (
      <PageHeader
        fullWidth={true}
        sourceIndicator={true}
        titleComponents={this.renderPageTitle}
        optionsComponents={this.optionsComponents}
        inPresentationMode={isHidden}
      />
    )
  }

  private get renderPageTitle(): JSX.Element {
    return (
      <>
        {this.dashboardSwitcher}
        {this.dashboardTitle}
      </>
    )
  }

  private get optionsComponents(): JSX.Element {
    const {
      handleChooseAutoRefresh,
      onManualRefresh,
      autoRefresh,
      handleChooseTimeRange,
      timeRange: {upper, lower},
      zoomedTimeRange: {upper: zoomedUpper, lower: zoomedLower},
    } = this.props

    return (
      <>
        <GraphTips />
        {this.addCellButton}
        <AutoRefreshDropdown
          onChoose={handleChooseAutoRefresh}
          onManualRefresh={onManualRefresh}
          selected={autoRefresh}
          iconName="refresh"
        />
        <TimeRangeDropdown
          onChooseTimeRange={handleChooseTimeRange}
          selected={{
            upper: zoomedUpper || upper,
            lower: zoomedLower || lower,
          }}
        />
        <button
          className="btn btn-default btn-sm btn-square"
          onClick={this.handleClickPresentationButton}
        >
          <span className="icon expand-a" />
        </button>
      </>
    )
  }

  private handleClickPresentationButton = (): void => {
    this.props.handleClickPresentationButton()
  }

  private get addCellButton(): JSX.Element {
    const {dashboard, onAddCell} = this.props

    if (dashboard) {
      return (
        <button className="btn btn-primary btn-sm" onClick={onAddCell}>
          <span className="icon plus" />
          Add Cell
        </button>
      )
    }
  }

  private get dashboardSwitcher(): JSX.Element {
    const {dashboardLinks} = this.props

    if (dashboardLinks.links.length > 1) {
      return <DashboardSwitcher dashboardLinks={dashboardLinks} />
    }
  }

  private get dashboardTitle(): JSX.Element {
    const {dashboard, activeDashboard, onRenameDashboard} = this.props

    if (dashboard) {
      return (
        <RenameDashboard onRename={onRenameDashboard} name={activeDashboard} />
      )
    }

    return <PageHeaderTitle title={activeDashboard} />
  }
}

export default DashboardHeader
