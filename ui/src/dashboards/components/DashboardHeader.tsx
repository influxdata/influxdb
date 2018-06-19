import React, {Component} from 'react'
import classnames from 'classnames'

import Authorized, {EDITOR_ROLE} from 'src/auth/Authorized'

import PageHeader from 'src/shared/components/PageHeader'
import PageHeaderTitle from 'src/shared/components/PageHeaderTitle'
import AutoRefreshDropdown from 'src/shared/components/AutoRefreshDropdown'
import TimeRangeDropdown from 'src/shared/components/TimeRangeDropdown'
import GraphTips from 'src/shared/components/GraphTips'
import DashboardHeaderEdit from 'src/dashboards/components/DashboardHeaderEdit'
import DashboardSwitcher from 'src/dashboards/components/DashboardSwitcher'
import {Dashboard, TimeRange} from 'src/types'

interface DashboardName {
  text: string
}

interface Props {
  activeDashboard: string
  dashboard: Dashboard
  onEditDashboard: () => void
  timeRange: TimeRange
  autoRefresh: number
  isEditMode?: boolean
  handleChooseTimeRange: (timeRange: TimeRange) => void
  handleChooseAutoRefresh: () => void
  onManualRefresh: () => void
  handleClickPresentationButton: () => void
  onAddCell: () => void
  onToggleTempVarControls: () => void
  showTemplateControlBar: boolean
  zoomedTimeRange: TimeRange
  onCancel: () => void
  onSave: () => void
  names: DashboardName[]
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
        renderTitle={this.title}
        renderOptions={this.options}
        inPresentationMode={isHidden}
      />
    )
  }

  private title = (): JSX.Element => {
    return (
      <>
        {this.dashboardSwitcher}
        {this.dashboardTitle}
      </>
    )
  }

  private options = (): JSX.Element => {
    const {
      handleChooseAutoRefresh,
      onManualRefresh,
      autoRefresh,
      handleChooseTimeRange,
      timeRange: {upper, lower},
      zoomedTimeRange: {upper: zoomedUpper, lower: zoomedLower},
      handleClickPresentationButton,
    } = this.props

    return (
      <>
        <GraphTips />
        {this.addCellButton}
        {this.tempVarsButton}
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
          onClick={handleClickPresentationButton}
        >
          <span className="icon expand-a" />
        </button>
      </>
    )
  }

  private get addCellButton(): JSX.Element {
    const {dashboard, onAddCell} = this.props

    if (dashboard) {
      return (
        <Authorized requiredRole={EDITOR_ROLE}>
          <button className="btn btn-primary btn-sm" onClick={onAddCell}>
            <span className="icon plus" />
            Add Cell
          </button>
        </Authorized>
      )
    }
  }

  private get tempVarsButton(): JSX.Element {
    const {
      dashboard,
      showTemplateControlBar,
      onToggleTempVarControls,
    } = this.props

    if (dashboard) {
      return (
        <div
          className={classnames('btn btn-default btn-sm', {
            active: showTemplateControlBar,
          })}
          onClick={onToggleTempVarControls}
        >
          <span className="icon cube" />Template Variables
        </div>
      )
    }
  }

  private get dashboardSwitcher(): JSX.Element {
    const {names, activeDashboard} = this.props

    if (names && names.length > 1) {
      return (
        <DashboardSwitcher names={names} activeDashboard={activeDashboard} />
      )
    }
  }

  private get dashboardTitle(): JSX.Element {
    const {
      dashboard,
      activeDashboard,
      onSave,
      onCancel,
      onEditDashboard,
      isEditMode,
    } = this.props

    if (dashboard) {
      return (
        <Authorized
          requiredRole={EDITOR_ROLE}
          replaceWithIfNotAuthorized={
            <PageHeaderTitle title={activeDashboard} />
          }
        >
          <DashboardHeaderEdit
            onSave={onSave}
            onCancel={onCancel}
            activeDashboard={activeDashboard}
            onEditDashboard={onEditDashboard}
            isEditMode={isEditMode}
          />
        </Authorized>
      )
    }

    return <PageHeaderTitle title={activeDashboard} />
  }
}

export default DashboardHeader
