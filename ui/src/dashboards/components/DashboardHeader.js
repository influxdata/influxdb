import React, {PropTypes} from 'react'

import AutoRefreshDropdown from 'shared/components/AutoRefreshDropdown'
import TimeRangeDropdown from 'shared/components/TimeRangeDropdown'
import SourceIndicator from '../../shared/components/SourceIndicator'
import GraphTips from '../../shared/components/GraphTips'

const DashboardHeader = ({
  children,
  buttonText,
  dashboard,
  headerText,
  timeRange,
  autoRefresh,
  isHidden,
  handleChooseTimeRange,
  handleChooseAutoRefresh,
  handleClickPresentationButton,
  source,
  onAddCell,
  onEditDashboard,
}) => isHidden ? null : (
  <div className="page-header full-width">
    <div className="page-header__container">
      <div className="page-header__left">
        {buttonText &&
          <div className="dropdown page-header-dropdown">
            <button className="dropdown-toggle" type="button" data-toggle="dropdown">
              <span className="button-text">{buttonText}</span>
              <span className="caret"></span>
            </button>
            <ul className="dropdown-menu" aria-labelledby="dropdownMenu1">
              {children}
            </ul>
          </div>
        }
        {headerText &&
          <h1>Kubernetes Dashboard</h1>
        }
      </div>
      <div className="page-header__right">
        <GraphTips />
        <SourceIndicator sourceName={source.name} />
        {
          dashboard ?
            <button className="btn btn-primary btn-sm" onClick={onAddCell}>
              <span className="icon plus" />
              Add Cell
            </button> : null
        }
        {
          dashboard ?
            <button className="btn btn-info btn-sm" onClick={onEditDashboard}>
              <span className="icon pencil" />
              &nbsp;Edit
            </button> : null
        }
        <AutoRefreshDropdown onChoose={handleChooseAutoRefresh} selected={autoRefresh} iconName="refresh" />
        <TimeRangeDropdown onChooseTimeRange={handleChooseTimeRange} selected={timeRange} />
        <div className="btn btn-info btn-sm" onClick={handleClickPresentationButton}>
          <span className="icon expand-a" style={{margin: 0}}></span>
        </div>
      </div>
    </div>
  </div>
)

const {
  array,
  bool,
  func,
  number,
  shape,
  string,
} = PropTypes

DashboardHeader.propTypes = {
  sourceID: string,
  children: array,
  buttonText: string,
  dashboard: shape({}),
  headerText: string,
  timeRange: shape({}).isRequired,
  autoRefresh: number.isRequired,
  isHidden: bool.isRequired,
  handleChooseTimeRange: func.isRequired,
  handleChooseAutoRefresh: func.isRequired,
  handleClickPresentationButton: func.isRequired,
  source: shape({}),
  onAddCell: func,
  onEditDashboard: func,
}

export default DashboardHeader
