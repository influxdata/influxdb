import React, {PropTypes} from 'react'
import {withRouter} from 'react-router'

import AutoRefreshDropdown from 'shared/components/AutoRefreshDropdown'
import CustomTimeRangeDropdown from 'shared/components/CustomTimeRangeDropdown'
import SourceIndicator from 'shared/components/SourceIndicator'
import GraphTips from 'shared/components/GraphTips'

const {func, number, shape, string} = PropTypes

const Header = React.createClass({
  propTypes: {
    actions: shape({
      handleChooseAutoRefresh: func.isRequired,
      setTimeRange: func.isRequired,
    }),
    autoRefresh: number.isRequired,
    showWriteForm: func.isRequired,
    timeRange: shape({
      lower: string,
      upper: string,
    }).isRequired,
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
    const {
      autoRefresh,
      actions: {handleChooseAutoRefresh},
      showWriteForm,
      timeRange,
    } = this.props

    return (
      <div className="page-header full-width">
        <div className="page-header__container">
          <div className="page-header__left">
            <h1 className="page-header__title">
              Data Explorer
            </h1>
          </div>
          <div className="page-header__right">
            <GraphTips />
            <SourceIndicator sourceName={this.context.source.name} />
            <div className="btn btn-sm btn-default" onClick={showWriteForm}>
              <span className="icon pencil" />
              Write Data
            </div>
            <AutoRefreshDropdown
              onChoose={handleChooseAutoRefresh}
              selected={autoRefresh}
              iconName="refresh"
            />
            <CustomTimeRangeDropdown
              onApplyTimeRange={this.handleChooseTimeRange}
              timeRange={timeRange}
            />
          </div>
        </div>
      </div>
    )
  },
})

export default withRouter(Header)
