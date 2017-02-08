import React, {PropTypes} from 'react';
import {bindActionCreators} from 'redux';
import {connect} from 'react-redux';

import QueryEditor from './QueryEditor';
import QueryTabItem from './QueryTabItem';
import SimpleDropdown from 'src/shared/components/SimpleDropdown';

import * as viewActions from '../actions/view';

const QueryBuilder = React.createClass({
  propTypes: {
    queries: PropTypes.arrayOf(PropTypes.shape({})).isRequired,
    timeRange: PropTypes.shape({
      upper: PropTypes.string,
      lower: PropTypes.string,
    }).isRequired,
    actions: PropTypes.shape({
      chooseNamespace: PropTypes.func.isRequired,
      chooseMeasurement: PropTypes.func.isRequired,
      chooseTag: PropTypes.func.isRequired,
      groupByTag: PropTypes.func.isRequired,
      addQuery: PropTypes.func.isRequired,
      deleteQuery: PropTypes.func.isRequired,
      toggleField: PropTypes.func.isRequired,
      groupByTime: PropTypes.func.isRequired,
      toggleTagAcceptance: PropTypes.func.isRequired,
      applyFuncsToField: PropTypes.func.isRequired,
    }).isRequired,
    setActiveQuery: PropTypes.func.isRequired,
    activeQueryID: PropTypes.string,
  },

  handleSetActiveQuery(query) {
    this.props.setActiveQuery(query.id);
  },

  handleAddQuery() {
    this.props.actions.addQuery();
  },

  handleAddRawQuery() {
    this.props.actions.addQuery({rawText: `SELECT "fields" from "db"."rp"."measurement"`});
  },

  handleDeleteQuery(query) {
    this.props.actions.deleteQuery(query.id);
  },

  getActiveQuery() {
    const {queries, activeQueryID} = this.props;
    const activeQuery = queries.find((query) => query.id === activeQueryID);
    const defaultQuery = queries[0];

    return activeQuery || defaultQuery;
  },

  render() {
    return (
      <div className="query-builder">
        {this.renderQueryTabList()}
        {this.renderQueryEditor()}
      </div>
    );
  },

  renderQueryEditor() {
    const {timeRange, actions} = this.props;
    const query = this.getActiveQuery();

    if (!query) {
      return (
        <div className="qeditor--empty">
          <h5>This Graph has no Queries</h5>
          <br/>
          <div className="btn btn-primary" role="button" onClick={this.handleAddQuery}>Add a Query</div>
        </div>
      );
    }

    return (
      <QueryEditor
        timeRange={timeRange}
        query={this.getActiveQuery()}
        actions={actions}
        onAddQuery={this.handleAddQuery}
      />
    );
  },

  renderQueryTabList() {
    const {queries} = this.props;
    return (
      <div className="query-builder--tabs">
        <div className="query-builder--tabs-heading">
          <h1>Queries</h1>
          {this.renderAddQuery()}
        </div>
        {queries.map((q) => {
          let queryTabText;
          if (q.rawText) {
            queryTabText = 'InfluxQL';
          } else {
            queryTabText = (q.measurement && q.fields.length !== 0) ? `${q.measurement}.${q.fields[0].field}` : 'Query';
          }
          return (
            <QueryTabItem
              isActive={this.getActiveQuery().id === q.id}
              key={q.id}
              query={q}
              onSelect={this.handleSetActiveQuery}
              onDelete={this.handleDeleteQuery}
              queryTabText={queryTabText}
            />
          );
        })}
      </div>
    );
  },

  onChoose(item) {
    switch (item.text) {
      case 'Query Builder':
        this.handleAddQuery();
        break;
      case 'InfluxQL':
        this.handleAddRawQuery();
        break;
    }
  },

  renderAddQuery() {
    return (
      <SimpleDropdown onChoose={this.onChoose} items={[{text: 'Query Builder'}, {text: 'InfluxQL'}]} className="panel--tab-new">
        <span className="icon plus"></span>
      </SimpleDropdown>
    );
  },
});

function mapStateToProps() {
  return {};
}

function mapDispatchToProps(dispatch) {
  return {
    actions: bindActionCreators(viewActions, dispatch),
  };
}

export default connect(mapStateToProps, mapDispatchToProps)(QueryBuilder);
