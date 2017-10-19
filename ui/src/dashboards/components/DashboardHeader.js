import React, {PropTypes} from 'react'
import classnames from 'classnames'

import AutoRefreshDropdown from 'shared/components/AutoRefreshDropdown'
import TimeRangeDropdown from 'shared/components/TimeRangeDropdown'
import SourceIndicator from 'shared/components/SourceIndicator'
import GraphTips from 'shared/components/GraphTips'
import DashboardHeaderEdit from 'src/dashboards/components/DashboardHeaderEdit'
import DashboardSwitcher from 'src/dashboards/components/DashboardSwitcher'

const DashboardHeader = ({
  hosts,
  onSave,
  sourceID,
  onCancel,
  isEditMode,
  isHidden,
  dashboard,
  onAddCell,
  dashboards,
  autoRefresh,
  dashboardName,
  onEditDashboard,
  onManualRefresh,
  handleChooseTimeRange,
  handleChooseAutoRefresh,
  onToggleTempVarControls,
  showTemplateControlBar,
  timeRange: {upper, lower},
  handleClickPresentationButton,
  zoomedTimeRange: {zoomedLower, zoomedUpper},
}) =>
  isHidden
    ? null
    : <div className="page-header full-width">
        <div className="page-header__container">
          <div
            className={
              dashboard
                ? 'page-header__left page-header__dash-editable'
                : 'page-header__left'
            }
          >
            {dashboards && dashboards.length > 1
              ? <DashboardSwitcher
                  dashboards={dashboards}
                  currentDashboard={dashboardName}
                  sourceID={sourceID}
                />
              : null}
            {hosts && hosts.length > 1
              ? <DashboardSwitcher
                  hosts={hosts}
                  currentDashboard={dashboardName}
                  sourceID={sourceID}
                />
              : null}
            {dashboard
              ? <DashboardHeaderEdit
                  onSave={onSave}
                  onCancel={onCancel}
                  dashboardName={dashboardName}
                  onEditDashboard={onEditDashboard}
                  isEditMode={isEditMode}
                />
              : <h1 className="page-header__title">
                  {dashboardName}
                </h1>}
          </div>
          <div className="page-header__right">
            <GraphTips />
            <SourceIndicator />
            {dashboard
              ? <button className="btn btn-primary btn-sm" onClick={onAddCell}>
                  <span className="icon plus" />
                  Add Cell
                </button>
              : null}
            {dashboard
              ? <div
                  className={classnames('btn btn-default btn-sm', {
                    active: showTemplateControlBar,
                  })}
                  onClick={onToggleTempVarControls}
                >
                  <span className="icon cube" />Template Variables
                </div>
              : null}
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
            <div
              className="btn btn-default btn-sm btn-square"
              onClick={handleClickPresentationButton}
            >
              <span className="icon expand-a" />
            </div>
          </div>
        </div>
      </div>

const {arrayOf, bool, func, number, shape, string} = PropTypes

DashboardHeader.defaultProps = {
  zoomedTimeRange: {
    zoomedLower: null,
    zoomedUpper: null,
  },
}

DashboardHeader.propTypes = {
  dashboardName: string.isRequired,
  onEditDashboard: func,
  dashboard: shape({}),
  timeRange: shape({
    lower: string,
    upper: string,
  }).isRequired,
  autoRefresh: number.isRequired,
  isHidden: bool.isRequired,
  isEditMode: bool,
  handleChooseTimeRange: func.isRequired,
  handleChooseAutoRefresh: func.isRequired,
  onManualRefresh: func.isRequired,
  handleClickPresentationButton: func.isRequired,
  onAddCell: func,
  onToggleTempVarControls: func,
  showTemplateControlBar: bool,
  zoomedTimeRange: shape({}),
  onCancel: func,
  onSave: func,
  dashboards: arrayOf(shape({})),
  sourceID: string,
  hosts: arrayOf(shape({})),
}

export default DashboardHeader
