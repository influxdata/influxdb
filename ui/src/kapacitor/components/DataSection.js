import React, {PropTypes} from 'react';
import classNames from 'classnames';
import _ from 'lodash';
import selectStatement from '../../chronograf/utils/influxql/select';

import DatabaseList from '../../chronograf/components/DatabaseList';
import MeasurementList from '../../chronograf/components/MeasurementList';
import FieldList from '../../chronograf/components/FieldList';
import TagList from '../../chronograf/components/TagList';

const DB_TAB = 'databases';
const MEASUREMENTS_TAB = 'measurments';
const FIELDS_TAB = 'fields';
const TAGS_TAB = 'tags';

export const DataSection = React.createClass({
  propTypes: {
    source: PropTypes.shape({
      links: PropTypes.shape({
        kapacitors: PropTypes.string.isRequired,
      }).isRequired,
    }),
    addFlashMessage: PropTypes.func,
  },

  childContextTypes: {
    source: PropTypes.shape({
      links: PropTypes.shape({
        proxy: PropTypes.string.isRequired,
        self: PropTypes.string.isRequired,
      }).isRequired,
    }).isRequired,
  },

  getChildContext() {
    return {source: this.props.source};
  },

  getInitialState() {
    return {
      activeTab: DB_TAB,
      query: {
        id: 1,
        database: null,
        measurement: null,
        retentionPolicy: null,
        fields: [],
        tags: {},
        groupBy: {
          time: null,
          tags: [],
        },
        areTagsAccepted: true,
        rawText: null,
      },
    };
  },

  handleChooseNamespace(namespace) {
    this.setState((oldState) => {
      const newQuery = Object.assign({}, oldState.query, namespace);
      return Object.assign({}, oldState, {query: newQuery, activeTab: MEASUREMENTS_TAB});
    });
  },

  handleChooseMeasurement(measurement) {
    this.setState((oldState) => {
      const newQuery = Object.assign({}, oldState.query, {measurement});
      return Object.assign({}, oldState, {query: newQuery, activeTab: FIELDS_TAB});
    });
  },

  handleToggleField({field, funcs}) {
    this.setState((oldState) => {
      const isSelected = oldState.query.fields.find((f) => f.field === field);
      if (isSelected) {
        const nextFields = oldState.query.fields.filter((f) => f.field !== field);
        if (!nextFields.length) {
          const nextGroupBy = Object.assign({}, oldState.query.groupBy, {time: null});
          return Object.assign({}, oldState, {
            query: Object.assign({}, oldState.query, {
              fields: nextFields,
              groupBy: nextGroupBy,
            }),
          });
        }

        return Object.assign({}, oldState, {
          query: Object.assign({}, oldState.query, {
            fields: nextFields,
          }),
        });
      }

      return Object.assign({}, oldState, {
        query: Object.assign({}, oldState.query, {
          fields: oldState.query.fields.concat({field, funcs}),
        }),
      })
          return update(queryConfig, {fields: {$push: [fieldFunc]}});
    });
  },

  handleGroupByTime(time) {
  },

  handleApplyFuncsToField(fieldFunc) {
  },

  handleChooseTag(tag) {
  },

  handleToggleTagAcceptance() {
  },

  handleGroupByTag(tagKey) {
  },

  handleClickTab(tab) {
    this.setState({activeTab: tab});
  },

  render() {
    const {query} = this.state;
    const timeRange = {lower: 'now() - 15m', upper: null};
    const statement = query.rawText || selectStatement(timeRange, query) || `SELECT "fields" FROM "db"."rp"."measurement"`;

    return (
      <div className="query-editor">
        <div className="query-editor__code">
          <pre className={classNames("", {"rq-mode": query.rawText})}><code>{statement}</code></pre>
        </div>
        {this.renderEditor()}
      </div>
    );
  },

  renderEditor() {
    const {query, activeTab} = this.state;
    if (query.rawText) {
      return (
        <div className="generic-empty-state query-editor-empty-state">
          <p className="margin-bottom-zero">
            <span className="icon alert-triangle"></span>
            &nbsp;Only editable in the <strong>Raw Query</strong> tab.
          </p>
        </div>
      );
    }

    return (
      <div>
        <div className="query-editor__tabs">
          <div className="query-editor__tabs-heading">Schema Explorer</div>
          <div onClick={_.wrap(DB_TAB, this.handleClickTab)} className={classNames("query-editor__tab", {active: activeTab === DB_TAB})}>Databases</div>
          <div onClick={_.wrap(MEASUREMENTS_TAB, this.handleClickTab)} className={classNames("query-editor__tab", {active: activeTab === MEASUREMENTS_TAB})}>Measurements</div>
          <div onClick={_.wrap(FIELDS_TAB, this.handleClickTab)} className={classNames("query-editor__tab", {active: activeTab === FIELDS_TAB})}>Fields</div>
          <div onClick={_.wrap(TAGS_TAB, this.handleClickTab)} className={classNames("query-editor__tab", {active: activeTab === TAGS_TAB})}>Tags</div>
        </div>
        {this.renderList()}
      </div>
    );
  },

  renderList() {
    const {query} = this.state;

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
        return <ul className="query-editor__list"></ul>;
    }
  },
});

export default DataSection;
