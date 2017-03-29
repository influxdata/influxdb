import React, {PropTypes} from 'react'
import {withRouter} from 'react-router'

import AutoRefreshDropdown from 'shared/components/AutoRefreshDropdown'
import TimeRangeDropdown from '../../shared/components/TimeRangeDropdown'
import SourceIndicator from '../../shared/components/SourceIndicator'

const {
  func,
  number,
  shape,
  string,
} = PropTypes

const Header = React.createClass({
  propTypes: {
    autoRefresh: number.isRequired,
    timeRange: shape({
      upper: string,
      lower: string,
    }).isRequired,
    actions: shape({
      handleChooseAutoRefresh: func.isRequired,
      setTimeRange: func.isRequired,
    }),
  },

  contextTypes: {
    source: shape({
      name: string,
    }),
  },

  handleChooseTimeRange(bounds) {
    this.props.actions.setTimeRange(bounds)
  },

  render() {
    const {autoRefresh, actions: {handleChooseAutoRefresh}, timeRange} = this.props

    return (
      <div className="page-header">
        <div className="page-header__container">
          <div className="page-header__left">
            <h1>Explorer</h1>
          </div>
          <div className="page-header__right">
            <SourceIndicator sourceName={this.context.source.name} />
            <AutoRefreshDropdown onChoose={handleChooseAutoRefresh} selected={autoRefresh} iconName="refresh" />
            <TimeRangeDropdown onChooseTimeRange={this.handleChooseTimeRange} selected={timeRange} />
          </div>
        </div>
      </div>
    )
  },
})

export default withRouter(Header)
