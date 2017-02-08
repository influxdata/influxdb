import React, {PropTypes} from 'react';
import _ from 'lodash';
import cx from 'classnames';

import TagListItem from './TagListItem';

import {showTagKeys, showTagValues} from 'shared/apis/metaQuery';
import showTagKeysParser from 'shared/parsing/showTagKeys';
import showTagValuesParser from 'shared/parsing/showTagValues';

const {string, shape, func, bool} = PropTypes;
const TagList = React.createClass({
  propTypes: {
    query: shape({
      database: string,
      measurement: string,
      retentionPolicy: string,
      areTagsAccepted: bool.isRequired,
    }).isRequired,
    onChooseTag: func.isRequired,
    onToggleTagAcceptance: func.isRequired,
    onGroupByTag: func.isRequired,
  },

  contextTypes: {
    source: PropTypes.shape({
      links: PropTypes.shape({
        proxy: PropTypes.string.isRequired,
      }).isRequired,
    }).isRequired,
  },

  getInitialState() {
    return {
      tags: {},
    };
  },

  _getTags() {
    const {database, measurement, retentionPolicy} = this.props.query;
    const {source} = this.context;
    const sourceProxy = source.links.proxy;

    showTagKeys({source: sourceProxy, database, retentionPolicy, measurement}).then((resp) => {
      const {errors, tagKeys} = showTagKeysParser(resp.data);
      if (errors.length) {
        // do something
      }

      return showTagValues({source: sourceProxy, database, retentionPolicy, measurement, tagKeys});
    }).then((resp) => {
      const {errors: errs, tags} = showTagValuesParser(resp.data);
      if (errs.length) {
        // do something
      }

      this.setState({tags});
    });
  },

  componentDidMount() {
    const {database, measurement, retentionPolicy} = this.props.query;
    if (!database || !measurement || !retentionPolicy) {
      return;
    }

    this._getTags();
  },

  componentDidUpdate(prevProps) {
    const {database, measurement, retentionPolicy} = this.props.query;
    const {database: prevDB, measurement: prevMeas, retentionPolicy: prevRP} = prevProps.query;
    if (!database || !measurement || !retentionPolicy) {
      return;
    }

    if (database === prevDB && measurement === prevMeas && retentionPolicy === prevRP) {
      return;
    }

    this._getTags();
  },

  handleAcceptReject(e) {
    e.stopPropagation();
    this.props.onToggleTagAcceptance();
  },

  render() {
    const {query} = this.props;

    return (
      <div className="query-builder--column">
        {(!query.database || !query.measurement || !query.retentionPolicy) ? null : <div className="qeditor--list-header">
          <div className="toggle toggle-sm">
            <div onClick={this.handleAcceptReject} className={cx("toggle-btn", {active: query.areTagsAccepted})}>Accept</div>
            <div onClick={this.handleAcceptReject} className={cx("toggle-btn", {active: !query.areTagsAccepted})}>Reject</div>
          </div>
        </div>}
        {this.renderList()}
      </div>
    );
  },

  renderList() {
    const {database, measurement, retentionPolicy} = this.props.query;
    if (!database || !measurement || !retentionPolicy) {
      return <div className="qeditor--empty">No <strong>Measurement</strong> selected</div>;
    }

    return (
      <ul className="qeditor--list">
        {_.map(this.state.tags, (tagValues, tagKey) => {
          return (
            <TagListItem
              key={tagKey}
              tagKey={tagKey}
              tagValues={tagValues}
              selectedTagValues={this.props.query.tags[tagKey] || []}
              isUsingGroupBy={this.props.query.groupBy.tags.indexOf(tagKey) > -1}
              onChooseTag={this.props.onChooseTag}
              onGroupByTag={this.props.onGroupByTag}
            />
          );
        })}
      </ul>
    );
  },
});

export default TagList;
