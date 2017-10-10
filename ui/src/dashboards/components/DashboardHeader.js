import React, {PropTypes} from 'react'
import classnames from 'classnames'
import _ from 'lodash'

import AutoRefreshDropdown from 'shared/components/AutoRefreshDropdown'
import TimeRangeDropdown from 'shared/components/TimeRangeDropdown'
import SourceIndicator from 'shared/components/SourceIndicator'
import GraphTips from 'shared/components/GraphTips'
import DashboardHeaderEdit from 'src/dashboards/components/DashboardHeaderEdit'

const DashboardHeader = ({
  onSave,
  children,
  onCancel,
  isEditMode,
  isHidden,
  dashboard,
  onAddCell,
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
            {children.length > 1
              ? <div className="dropdown dashboard-switcher">
                  <button
                    className="btn btn-square btn-default btn-sm dropdown-toggle"
                    type="button"
                    data-toggle="dropdown"
                  >
                    <span className="icon dash-f" />
                  </button>
                  <ul className="dropdown-menu">
                    {_.sortBy(children, c =>
                      c.props.children.props.children.toLowerCase()
                    )}
                  </ul>
                </div>
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
              <span className="icon expand-a" style={{margin: 0}} />
            </div>
          </div>
        </div>
      </div>

const {array, bool, func, number, shape, string} = PropTypes

DashboardHeader.defaultProps = {
  zoomedTimeRange: {
    zoomedLower: null,
    zoomedUpper: null,
  },
}

DashboardHeader.propTypes = {
  children: array,
  dashboardName: string.isRequired,
  onEditDashboard: func.isRequired,
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
  onCancel: func.isRequired,
  onSave: func.isRequired,
}

export default DashboardHeader
