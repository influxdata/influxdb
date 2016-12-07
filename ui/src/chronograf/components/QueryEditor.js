import React, {PropTypes} from 'react';
import classNames from 'classnames';
import _ from 'lodash';
import selectStatement from '../utils/influxql/select';

import DatabaseList from './DatabaseList';
import MeasurementList from './MeasurementList';
import FieldList from './FieldList';
import TagList from './TagList';

const DB_TAB = 'databases';
const MEASUREMENTS_TAB = 'measurments';
const FIELDS_TAB = 'fields';
const TAGS_TAB = 'tags';

const {string, shape, func} = PropTypes;
const QueryEditor = React.createClass({
  propTypes: {
    query: shape({
      id: string.isRequired,
    }).isRequired,
    timeRange: shape({
      upper: string,
      lower: string,
    }).isRequired,
    actions: shape({
      chooseNamespace: func.isRequired,
      chooseMeasurement: func.isRequired,
      applyFuncsToField: func.isRequired,
      chooseTag: func.isRequired,
      groupByTag: func.isRequired,
      toggleField: func.isRequired,
      groupByTime: func.isRequired,
      toggleTagAcceptance: func.isRequired,
    }).isRequired,
  },

  getInitialState() {
    return {
      activeTab: DB_TAB,
      database: null,
      measurement: null,
    };
  },

  componentWillReceiveProps(nextProps) {
    const changingQueries = this.props.query.id !== nextProps.query.id;
    if (changingQueries) {
      this.setState({activeTab: DB_TAB});
    }
  },

  handleChooseNamespace(namespace) {
    this.props.actions.chooseNamespace(this.props.query.id, namespace);

    this.setState({activeTab: MEASUREMENTS_TAB});
  },

  handleChooseMeasurement(measurement) {
    this.props.actions.chooseMeasurement(this.props.query.id, measurement);

    this.setState({activeTab: FIELDS_TAB});
  },

  handleToggleField(field) {
    this.props.actions.toggleField(this.props.query.id, field);
  },

  handleGroupByTime(time) {
    this.props.actions.groupByTime(this.props.query.id, time);
  },

  handleApplyFuncsToField(fieldFunc) {
    this.props.actions.applyFuncsToField(this.props.query.id, fieldFunc);
  },

  handleChooseTag(tag) {
    this.props.actions.chooseTag(this.props.query.id, tag);
  },

  handleToggleTagAcceptance() {
    this.props.actions.toggleTagAcceptance(this.props.query.id);
  },

  handleGroupByTag(tagKey) {
    this.props.actions.groupByTag(this.props.query.id, tagKey);
  },

  handleEditRawText(_queryID, text) {
    this.props.actions.editRawText(this.props.query.id, text);
  },

  handleClickTab(tab) {
    this.setState({activeTab: tab});
  },

  render() {
    return (
      <div className="explorer--tab-contents">
        {this.renderQuery()}
        {this.renderLists()}
      </div>
    );
  },

  renderQuery() {
    const {query, timeRange} = this.props;
    const statement = query.rawText || selectStatement(timeRange, query) || `SELECT "fields" FROM "db"."rp"."measurement"`;

    if (!query.rawText) {
      return (
        <div className="qeditor--query-preview">
          <pre><code>{statement}</code></pre>
        </div>
      );
    }

    return <RawQueryEditor query={query} onUpdate={this.handleEditRawText} defaultValue={query.rawText} />;
  },

  renderLists() {
    const {activeTab} = this.state;
    return (
      <div>
        <div className="qeditor--tabs">
          <div className="qeditor--tabs-heading">Schema Explorer</div>
          <div onClick={_.wrap(DB_TAB, this.handleClickTab)} className={classNames("qeditor--tab", {active: activeTab === DB_TAB})}>Databases</div>
          <div onClick={_.wrap(MEASUREMENTS_TAB, this.handleClickTab)} className={classNames("qeditor--tab", {active: activeTab === MEASUREMENTS_TAB})}>Measurements</div>
          <div onClick={_.wrap(FIELDS_TAB, this.handleClickTab)} className={classNames("qeditor--tab", {active: activeTab === FIELDS_TAB})}>Fields</div>
          <div onClick={_.wrap(TAGS_TAB, this.handleClickTab)} className={classNames("qeditor--tab", {active: activeTab === TAGS_TAB})}>Tags</div>
        </div>
        {this.renderList()}
      </div>
    );
  },

  renderList() {
    const {query} = this.props;

    switch (this.state.activeTab) {
      case DB_TAB:
        return (
          <DatabaseList
            query={query}
            onChooseNamespace={this.handleChooseNamespace}
          />
        );
      case MEASUREMENTS_TAB:
        return (
          <MeasurementList
            query={query}
            onChooseMeasurement={this.handleChooseMeasurement}
          />
        );
      case FIELDS_TAB:
        return (
          <FieldList
            query={query}
            onToggleField={this.handleToggleField}
            onGroupByTime={this.handleGroupByTime}
            applyFuncsToField={this.handleApplyFuncsToField}
          />
        );
      case TAGS_TAB:
        return (
          <TagList
            query={query}
            onChooseTag={this.handleChooseTag}
            onGroupByTag={this.handleGroupByTag}
            onToggleTagAcceptance={this.handleToggleTagAcceptance}
          />
        );
      default:
        return <ul className="qeditor--list"></ul>;
    }
  },
});

const ENTER = 13;
const RawQueryEditor = React.createClass({
  propTypes: {
    query: PropTypes.shape({
      rawText: PropTypes.string,
      id: PropTypes.string.isRequired,
    }).isRequired,
    onUpdate: PropTypes.func.isRequired,
    defaultValue: PropTypes.string.isRequired,
  },

  handleKeyDown(e) {
    e.stopPropagation();
    if (e.keyCode !== ENTER) {
      return;
    }
    e.preventDefault();
    this.editor.blur();
  },

  handleUpdate() {
    const text = this.editor.value;
    this.props.onUpdate(this.props.query.id, text);
  },

  render() {
    const {defaultValue} = this.props;

    return (
      <div className="raw-query-editor-wrapper rq-mode">
        <textarea
          className="raw-query-editor"
          onKeyDown={this.handleKeyDown}
          onBlur={this.handleUpdate}
          ref={(editor) => this.editor = editor}
          defaultValue={defaultValue}
          placeholder="Blank query"
        />
      </div>
    );
  },
});

export default QueryEditor;
