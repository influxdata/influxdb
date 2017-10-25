import React, {PropTypes} from 'react'
import {withRouter} from 'react-router'

import Authorized, {EDITOR_ROLE} from 'src/auth/Authorized'

import AutoRefreshDropdown from 'shared/components/AutoRefreshDropdown'
import TimeRangeDropdown from 'shared/components/TimeRangeDropdown'
import SourceIndicator from 'shared/components/SourceIndicator'
import GraphTips from 'shared/components/GraphTips'

const {func, number, shape, string} = PropTypes

const Header = ({
  timeRange,
  autoRefresh,
  showWriteForm,
  onManualRefresh,
  onChooseTimeRange,
  onChooseAutoRefresh,
}) =>
  <div className="page-header full-width">
    <div className="page-header__container">
      <div className="page-header__left">
        <h1 className="page-header__title">Data Explorer</h1>
      </div>
      <div className="page-header__right">
        <GraphTips />
        <SourceIndicator />
        <Authorized requiredRole={EDITOR_ROLE}>
          <div
            className="btn btn-sm btn-default"
            onClick={showWriteForm}
            data-test="write-data-button"
          >
            <span className="icon pencil" />
            Write Data
          </div>
        </Authorized>
        <AutoRefreshDropdown
          iconName="refresh"
          selected={autoRefresh}
          onChoose={onChooseAutoRefresh}
          onManualRefresh={onManualRefresh}
        />
        <TimeRangeDropdown
          selected={timeRange}
          page="DataExplorer"
          onChooseTimeRange={onChooseTimeRange}
        />
      </div>
    </div>
  </div>

Header.propTypes = {
  onChooseAutoRefresh: func.isRequired,
  onChooseTimeRange: func.isRequired,
  onManualRefresh: func.isRequired,
  autoRefresh: number.isRequired,
  showWriteForm: func.isRequired,
  timeRange: shape({
    lower: string,
    upper: string,
  }).isRequired,
}

export default withRouter(Header)
