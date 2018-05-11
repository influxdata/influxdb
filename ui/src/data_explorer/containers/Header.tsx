import React, {PureComponent} from 'react'

import AutoRefreshDropdown from 'src/shared/components/AutoRefreshDropdown'
import TimeRangeDropdown from 'src/shared/components/TimeRangeDropdown'
import SourceIndicator from 'src/shared/components/SourceIndicator'
import GraphTips from 'src/shared/components/GraphTips'
import {TimeRange} from 'src/types'

interface Props {
  onChooseAutoRefresh: () => void
  onManualRefresh: () => void
  onChooseTimeRange: (timeRange: TimeRange) => void
  showWriteForm: () => void
  autoRefresh: number
  timeRange: TimeRange
}

class Header extends PureComponent<Props> {
  public render() {
    const {
      timeRange,
      autoRefresh,
      showWriteForm,
      onManualRefresh,
      onChooseTimeRange,
      onChooseAutoRefresh,
    } = this.props
    return (
      <div className="page-header full-width">
        <div className="page-header__container">
          <div className="page-header__left">
            <h1 className="page-header__title">Data Explorer</h1>
          </div>
          <div className="page-header__right">
            <GraphTips />
            <SourceIndicator />
            <div
              className="btn btn-sm btn-default"
              onClick={showWriteForm}
              data-test="write-data-button"
            >
              <span className="icon pencil" />
              Write Data
            </div>
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
    )
  }
}

export default Header
