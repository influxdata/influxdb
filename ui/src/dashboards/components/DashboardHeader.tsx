// Libraries
import React, {Component} from 'react'

// Components
import {Page} from 'src/pageLayout'
import AutoRefreshDropdown from 'src/shared/components/dropdown_auto_refresh/AutoRefreshDropdown'
import TimeRangeDropdown from 'src/shared/components/TimeRangeDropdown'
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

interface DefaultProps {
  zoomedTimeRange: QueriesModels.TimeRange
}

interface Props extends DefaultProps {
  activeDashboard: string
  dashboard: Dashboard
  timeRange: QueriesModels.TimeRange
  autoRefresh: number
  handleChooseTimeRange: (timeRange: QueriesModels.TimeRange) => void
  handleChooseAutoRefresh: AppActions.SetAutoRefreshActionCreator
  onManualRefresh: () => void
  handleClickPresentationButton: AppActions.DelayEnablePresentationModeDispatcher
  onAddCell: () => void
  onRenameDashboard: (name: string) => Promise<void>
  toggleVariablesControlBar: () => void
  isShowingVariablesControlBar: boolean
  isHidden: boolean
  onAddNote: () => void
}

export default class DashboardHeader extends Component<Props> {
  public static defaultProps: DefaultProps = {
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
      toggleVariablesControlBar,
      isShowingVariablesControlBar,
      onAddCell,
      onRenameDashboard,
      activeDashboard,
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
            onSetTimeRange={handleChooseTimeRange}
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
}
