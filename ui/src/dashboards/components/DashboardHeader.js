import React, {PropTypes} from 'react'
import ReactTooltip from 'react-tooltip'

import TimeRangeDropdown from 'shared/components/TimeRangeDropdown'

const DashboardHeader = ({
  children,
  buttonText,
  headerText,
  timeRange,
  isHidden,
  handleChooseTimeRange,
  handleClickPresentationButton,
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
        <div className="btn btn-info btn-sm" data-for="graph-tips-tooltip" data-tip="<p><code>Click + Drag</code> Zoom in (X or Y)</p><p><code>Shift + Click</code> Pan Graph Window</p><p><code>Double Click</code> Reset Graph Window</p>">
          <span className="icon heart"></span>
          Graph Tips
        </div>
        <ReactTooltip id="graph-tips-tooltip" effect="solid" html={true} offset={{top: 2}} place="bottom" class="influx-tooltip place-bottom" />
        <TimeRangeDropdown onChooseTimeRange={handleChooseTimeRange} selected={timeRange.inputValue} />
        <div className="btn btn-sm btn-info" onClick={handleClickPresentationButton}>
          <span className="icon keynote" style={{margin: 0}}></span>
        </div>
      </div>
    </div>
  </div>
)

const {
  shape,
  array,
  string,
  func,
  bool,
} = PropTypes

DashboardHeader.propTypes = {
  children: array,
  buttonText: string,
  headerText: string,
  timeRange: shape({}).isRequired,
  isHidden: bool.isRequired,
  handleChooseTimeRange: func.isRequired,
  handleClickPresentationButton: func.isRequired,
}

export default DashboardHeader
