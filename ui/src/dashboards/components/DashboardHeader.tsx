// Libraries
import React, {Component, createRef, RefObject} from 'react'
import _ from 'lodash'

// Components
import AutoRefreshDropdown from 'src/shared/components/dropdown_auto_refresh/AutoRefreshDropdown'
import TimeRangeDropdown, {
  RangeType,
} from 'src/shared/components/TimeRangeDropdown'
import GraphTips from 'src/shared/components/graph_tips/GraphTips'
import RenamablePageTitle from 'src/pageLayout/components/RenamablePageTitle'
import TimeZoneDropdown from 'src/shared/components/TimeZoneDropdown'
import {
  SquareButton,
  Button,
  IconFont,
  ComponentColor,
  Page,
  Popover, PopoverPosition, PopoverType
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
import {AutoRefresh, AutoRefreshStatus, Organization} from 'src/types'

interface Props {
  org: Organization
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
  onRenameDashboard: (name: string) => void
  toggleVariablesControlBar: () => void
  isShowingVariablesControlBar: boolean
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
    const triggerRef: createRef<HTMLSpanElement>()

    const {
      org,
      handleChooseAutoRefresh,
      onManualRefresh,
      timeRange,
      timeRange: {upper, lower},
      zoomedTimeRange: {upper: zoomedUpper, lower: zoomedLower},
      toggleVariablesControlBar,
      isShowingVariablesControlBar,
      onAddCell,
      onRenameDashboard,
      activeDashboard,
      autoRefresh,
    } = this.props

    return (
      <Page.Header fullWidth={true}>
        <Page.HeaderLeft>
          <RenamablePageTitle
            prefix={_.get(org, 'name', '')}
            maxLength={DASHBOARD_NAME_MAX_LENGTH}
            onRename={onRenameDashboard}
            name={activeDashboard}
            placeholder={DEFAULT_DASHBOARD_NAME}
          />
        </Page.HeaderLeft>
        <Page.HeaderRight>
        <span ref={triggerRef}>?</span>
        <Popover
            type={PopoverType.Outline}
            position={PopoverPosition.Below}
            triggerRef={triggerRef}
            distanceFromTrigger={8}
            contents={()=> (
              <span>
            <h1>Graph Tips:</h1><p><code>Click + Drag</code> Zoom in (X or Y)<br/><code>Shift + Click</code> Pan Graph Window<br/><code>Double Click</code> Reset Graph Window</p><h1>Static Legend Tips:</h1><p><code>Click</code>Focus on single Series<br/><code>Shift + Click</code> Show/Hide single Series</p>
              </span>
            )}
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
            timeRange={{
              ...timeRange,
              upper: zoomedUpper || upper,
              lower: zoomedLower || lower,
            }}
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
