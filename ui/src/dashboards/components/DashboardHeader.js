import React, {PropTypes} from 'react'
import classnames from 'classnames'

import AutoRefreshDropdown from 'shared/components/AutoRefreshDropdown'
import TimeRangeDropdown from 'shared/components/TimeRangeDropdown'
import SourceIndicator from 'shared/components/SourceIndicator'
import GraphTips from 'shared/components/GraphTips'

const DashboardHeader = ({
  children,
  buttonText,
  dashboard,
  headerText,
  timeRange: {upper, lower},
  zoomedTimeRange: {zoomedLower, zoomedUpper},
  autoRefresh,
  isHidden,
  handleChooseTimeRange,
  handleChooseAutoRefresh,
  handleClickPresentationButton,
  onAddCell,
  onEditDashboard,
  onToggleTempVarControls,
  showTemplateControlBar,
}) =>
  isHidden
    ? null
    : <div className="page-header full-width">
        <div className="page-header__container">
          <div className="page-header__left">
            {buttonText &&
              <div className="dropdown page-header-dropdown">
                <button
                  className="dropdown-toggle"
                  type="button"
                  data-toggle="dropdown"
                >
                  <span>
                    {buttonText}
                  </span>
                  <span className="caret" />
                </button>
                <ul className="dropdown-menu">
                  {children}
                </ul>
              </div>}
            {headerText}
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
              ? <button
                  className="btn btn-default btn-sm"
                  onClick={onEditDashboard}
                >
                  <span className="icon pencil" />
                  Rename
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
  buttonText: string,
  dashboard: shape({}),
  headerText: string,
  timeRange: shape({
    lower: string,
    upper: string,
  }).isRequired,
  autoRefresh: number.isRequired,
  isHidden: bool.isRequired,
  handleChooseTimeRange: func.isRequired,
  handleChooseAutoRefresh: func.isRequired,
  handleClickPresentationButton: func.isRequired,
  onAddCell: func,
  onEditDashboard: func,
  onToggleTempVarControls: func,
  showTemplateControlBar: bool,
  zoomedTimeRange: shape({}),
}

export default DashboardHeader
