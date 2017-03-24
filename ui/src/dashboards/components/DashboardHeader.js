import React, {PropTypes} from 'react'
import ReactTooltip from 'react-tooltip'

import AutoRefreshDropdown from 'shared/components/AutoRefreshDropdown'
import TimeRangeDropdown from 'shared/components/TimeRangeDropdown'
import SourceIndicator from '../../shared/components/SourceIndicator'

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
        {
          dashboard ?
            <button className="btn btn-info btn-sm" onClick={onAddCell}>
              <span className="icon plus" />
              &nbsp;Add Cell
            </button> : null
        }
        {
          dashboard ?
            <button className="btn btn-info btn-sm" onClick={onEditDashboard}>
              <span className="icon pencil" />
              &nbsp;Edit
            </button> : null
        }
        <div className="btn btn-info btn-sm" data-for="graph-tips-tooltip" data-tip="<p><code>Click + Drag</code> Zoom in (X or Y)</p><p><code>Shift + Click</code> Pan Graph Window</p><p><code>Double Click</code> Reset Graph Window</p>">
          <span className="icon heart"></span>
          Graph Tips
        </div>
        <ReactTooltip id="graph-tips-tooltip" effect="solid" html={true} offset={{top: 2}} place="bottom" class="influx-tooltip place-bottom" />
        <SourceIndicator sourceName={source.name} />
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
  onAddCell: func.isRequired,
  onEditDashboard: func.isRequired,
}

export default DashboardHeader
