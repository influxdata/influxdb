// Libraries
import React, {Component} from 'react'
import {connect} from 'react-redux'

// Components
import {Page} from 'src/pageLayout'
import AutoRefreshDropdown from 'src/shared/components/dropdown_auto_refresh/AutoRefreshDropdown'
import TimeRangeDropdown from 'src/shared/components/TimeRangeDropdown'
import GraphTips from 'src/shared/components/graph_tips/GraphTips'
import RenameDashboard from 'src/dashboards/components/rename_dashboard/RenameDashboard'
import {Button, ButtonShape, ComponentColor, IconFont} from 'src/clockface'

// Actions
import {addNote} from 'src/dashboards/actions/v2/notes'

// Types
import * as AppActions from 'src/types/actions/app'
import * as QueriesModels from 'src/types/queries'
import {Dashboard} from 'src/api'
import {DashboardSwitcherLinks} from 'src/types/v2/dashboards'

interface OwnProps {
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

interface DispatchProps {
  onAddNote: typeof addNote
}

type Props = OwnProps & DispatchProps

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
      onAddNote,
    } = this.props

    return (
      <Page.Header fullWidth={true} inPresentationMode={isHidden}>
        <Page.Header.Left>{this.dashboardTitle}</Page.Header.Left>
        <Page.Header.Right>
          <GraphTips />
          {this.addCellButton}
          <Button
            icon={IconFont.TextBlock}
            text="Add Note"
            onClick={onAddNote}
          />
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
          icon={IconFont.AddCell}
          color={ComponentColor.Primary}
          onClick={onAddCell}
          text="Add Cell"
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

const mdtp = {
  onAddNote: addNote,
}

export default connect<{}, DispatchProps, OwnProps>(
  null,
  mdtp
)(DashboardHeader)
