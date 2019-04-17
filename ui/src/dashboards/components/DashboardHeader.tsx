// Libraries
import React, {Component} from 'react'

// Components
import {Page} from 'src/pageLayout'
import AutoRefreshDropdown from 'src/shared/components/dropdown_auto_refresh/AutoRefreshDropdown'
import TimeRangeDropdown, {
  RangeType,
} from 'src/shared/components/TimeRangeDropdown'
import GraphTips from 'src/shared/components/graph_tips/GraphTips'
import RenamablePageTitle from 'src/pageLayout/components/RenamablePageTitle'
import {
  Button,
  IconFont,
  ButtonShape,
  ComponentColor,
} from '@influxdata/clockface'

// Constants
import {
  DEFAULT_DASHBOARD_NAME,
  DASHBOARD_NAME_MAX_LENGTH,
} from 'src/dashboards/constants/index'

// Types
import * as AppActions from 'src/types/actions/app'
import * as QueriesModels from 'src/types/queries'
import {Dashboard} from '@influxdata/influx'
import {AutoRefresh, AutoRefreshStatus} from 'src/types'

interface Props {
  activeDashboard: string
  dashboard: Dashboard
  timeRange: QueriesModels.TimeRange
  autoRefresh: AutoRefresh
  handleChooseTimeRange: (timeRange: QueriesModels.TimeRange) => void
  handleChooseAutoRefresh: (autoRefreshInterval: number) => void
  onSetAutoRefreshStatus: (status: AutoRefreshStatus) => void
  onManualRefresh: () => void
  handleClickPresentationButton: AppActions.DelayEnablePresentationModeDispatcher
  onAddCell: () => void
  onRenameDashboard: (name: string) => Promise<void>
  toggleVariablesControlBar: () => void
  isShowingVariablesControlBar: boolean
  isHidden: boolean
  onAddNote: () => void
  zoomedTimeRange: QueriesModels.TimeRange
}

export default class DashboardHeader extends Component<Props> {
  public static defaultProps = {
    zoomedTimeRange: {
      upper: null,
      lower: null,
    },
  }

  public render() {
    const {
      handleChooseAutoRefresh,
      onManualRefresh,
      timeRange: {upper, lower},
      zoomedTimeRange: {upper: zoomedUpper, lower: zoomedLower},
      isHidden,
      toggleVariablesControlBar,
      isShowingVariablesControlBar,
      onAddCell,
      onRenameDashboard,
      activeDashboard,
      autoRefresh,
    } = this.props

    return (
      <Page.Header fullWidth={true} inPresentationMode={isHidden}>
        <Page.Header.Left>
          <RenamablePageTitle
            maxLength={DASHBOARD_NAME_MAX_LENGTH}
            onRename={onRenameDashboard}
            name={activeDashboard}
            placeholder={DEFAULT_DASHBOARD_NAME}
          />
        </Page.Header.Left>
        <Page.Header.Right>
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
          <AutoRefreshDropdown
            onChoose={handleChooseAutoRefresh}
            onManualRefresh={onManualRefresh}
            selected={autoRefresh}
          />
          <TimeRangeDropdown
            onSetTimeRange={this.handleChooseTimeRange}
            timeRange={{
              upper: zoomedUpper || upper,
              lower: zoomedLower || lower,
            }}
          />
          <Button
            text="Variables"
            onClick={toggleVariablesControlBar}
            color={
              isShowingVariablesControlBar
                ? ComponentColor.Primary
                : ComponentColor.Default
            }
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

  private handleAddNote = () => {
    this.props.onAddNote()
  }

  private handleClickPresentationButton = (): void => {
    this.props.handleClickPresentationButton()
  }

  private handleChooseTimeRange = (
    timeRange: QueriesModels.TimeRange,
    rangeType: RangeType = RangeType.Relative
  ) => {
    const {
      autoRefresh,
      onSetAutoRefreshStatus,
      handleChooseTimeRange,
    } = this.props

    handleChooseTimeRange(timeRange)

    if (rangeType === RangeType.Absolute) {
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
