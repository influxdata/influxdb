import React, {PropTypes} from 'react'
import {connect} from 'react-redux'
import {bindActionCreators} from 'redux'

import _ from 'lodash'

import QueryBuilder from '../components/QueryBuilder'
import Visualization from '../components/Visualization'
import Header from '../containers/Header'
import ResizeContainer, {ResizeBottom} from 'src/shared/components/ResizeContainer'

import {setAutoRefresh} from 'shared/actions/app'
import * as viewActions from 'src/data_explorer/actions/view'

const {
  arrayOf,
  func,
  number,
  shape,
  string,
} = PropTypes

const DataExplorer = React.createClass({
  propTypes: {
    source: shape({
      links: shape({
        proxy: string.isRequired,
        self: string.isRequired,
      }).isRequired,
    }).isRequired,
    queryConfigs: arrayOf(shape({})).isRequired,
    queryConfigActions: shape({}).isRequired,
    autoRefresh: number.isRequired,
    handleChooseAutoRefresh: func.isRequired,
    timeRange: shape({
      upper: string,
      lower: string,
    }).isRequired,
    setTimeRange: func.isRequired,
    dataExplorer: shape({
      queryIDs: arrayOf(string).isRequired,
    }).isRequired,
  },

  childContextTypes: {
    source: shape({
      links: shape({
        proxy: string.isRequired,
        self: string.isRequired,
      }).isRequired,
    }).isRequired,
  },

  getChildContext() {
    return {source: this.props.source}
  },

  getInitialState() {
    return {
      activeQueryIndex: null,
    }
  },

  handleSetActiveQueryIndex(index) {
    this.setState({activeQueryIndex: index})
  },

  handleDeleteQuery(index) {
    const {queryConfigs} = this.props
    const query = queryConfigs[index]
    this.props.queryConfigActions.deleteQuery(query.id)
  },

  render() {
    const {autoRefresh, handleChooseAutoRefresh, timeRange, setTimeRange, queryConfigs, queryConfigActions} = this.props
    const {activeQueryIndex} = this.state

    return (
      <div className="data-explorer">
        <Header
          actions={{handleChooseAutoRefresh, setTimeRange}}
          autoRefresh={autoRefresh}
          timeRange={timeRange}
        />
        <ResizeContainer>
          <Visualization
            autoRefresh={autoRefresh}
            timeRange={timeRange}
            queryConfigs={queryConfigs}
            activeQueryIndex={activeQueryIndex}
            onEditRawStatus={queryConfigActions.editRawQueryStatus}
          />
          <ResizeBottom>
            <QueryBuilder
              queries={queryConfigs}
              actions={queryConfigActions}
              autoRefresh={autoRefresh}
              timeRange={timeRange}
              setActiveQueryIndex={this.handleSetActiveQueryIndex}
              onDeleteQuery={this.handleDeleteQuery}
              activeQueryIndex={activeQueryIndex}
            />
          </ResizeBottom>
        </ResizeContainer>
      </div>
    )
  },
})

function mapStateToProps(state) {
  const {app: {persisted: {autoRefresh}}, timeRange, queryConfigs, dataExplorer} = state
  const queryConfigValues = _.values(queryConfigs)

  return {
    autoRefresh,
    timeRange,
    queryConfigs: queryConfigValues,
    dataExplorer,
  }
}

function mapDispatchToProps(dispatch) {
  return {
    handleChooseAutoRefresh: bindActionCreators(setAutoRefresh, dispatch),
    setTimeRange: bindActionCreators(viewActions.setTimeRange, dispatch),
    queryConfigActions: bindActionCreators(viewActions, dispatch),
  }
}

export default connect(mapStateToProps, mapDispatchToProps)(DataExplorer)
