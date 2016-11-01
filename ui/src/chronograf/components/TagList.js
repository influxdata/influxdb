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

  componentDidMount() {
    const {database, measurement, retentionPolicy} = this.props.query;
    const {source} = this.context;
    if (!database || !measurement || !retentionPolicy) {
      return;
    }

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

  handleAcceptReject(e) {
    e.stopPropagation();
    this.props.onToggleTagAcceptance();
  },

  render() {
    const {query} = this.props;

    return (
      <div>
        <div className="query-editor__list-header">
          <div className="tag-list__toggle">
            <div onClick={this.handleAcceptReject} className={cx("tag-list__toggle-btn", {active: query.areTagsAccepted})}>Accept</div>
            <div onClick={this.handleAcceptReject} className={cx("tag-list__toggle-btn", {active: !query.areTagsAccepted})}>Reject</div>
          </div>
        </div>
        {this.renderList()}
      </div>
    );
  },

  renderList() {
    const {database, measurement, retentionPolicy} = this.props.query;
    if (!database || !measurement || !retentionPolicy) {
      return <div className="query-editor__empty">No measurement selected.</div>;
    }

    return (
      <ul className="query-editor__list">
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
