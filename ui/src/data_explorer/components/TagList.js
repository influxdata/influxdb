import React, {PropTypes} from 'react'
import _ from 'lodash'

import TagListItem from './TagListItem'

import {showTagKeys, showTagValues} from 'shared/apis/metaQuery'
import showTagKeysParser from 'shared/parsing/showTagKeys'
import showTagValuesParser from 'shared/parsing/showTagValues'

const {string, shape, func, bool} = PropTypes

const TagList = React.createClass({
  propTypes: {
    query: shape({
      database: string,
      measurement: string,
      retentionPolicy: string,
      areTagsAccepted: bool.isRequired,
    }).isRequired,
    onChooseTag: func.isRequired,
    onGroupByTag: func.isRequired,
    querySource: shape({
      links: shape({
        proxy: string.isRequired,
      }).isRequired,
    }),
  },

  contextTypes: {
    source: shape({
      links: shape({
        proxy: string.isRequired,
      }).isRequired,
    }).isRequired,
  },

  getDefaultProps() {
    return {
      querySource: null,
    }
  },

  getInitialState() {
    return {
      tags: {},
    }
  },

  _getTags() {
    const {database, measurement, retentionPolicy} = this.props.query
    const {source} = this.context
    const {querySource} = this.props

    const proxy =
      _.get(querySource, ['links', 'proxy'], null) || source.links.proxy

    showTagKeys({source: proxy, database, retentionPolicy, measurement})
      .then(resp => {
        const {errors, tagKeys} = showTagKeysParser(resp.data)
        if (errors.length) {
          // do something
        }

        return showTagValues({
          source: proxy,
          database,
          retentionPolicy,
          measurement,
          tagKeys,
        })
      })
      .then(resp => {
        const {errors: errs, tags} = showTagValuesParser(resp.data)
        if (errs.length) {
          // do something
        }

        this.setState({tags})
      })
  },

  componentDidMount() {
    const {database, measurement, retentionPolicy} = this.props.query
    if (!database || !measurement || !retentionPolicy) {
      return
    }

    this._getTags()
  },

  componentDidUpdate(prevProps) {
    const {query, querySource} = this.props
    const {database, measurement, retentionPolicy} = query

    const {
      database: prevDB,
      measurement: prevMeas,
      retentionPolicy: prevRP,
    } = prevProps.query
    if (!database || !measurement || !retentionPolicy) {
      return
    }

    if (
      database === prevDB &&
      measurement === prevMeas &&
      retentionPolicy === prevRP &&
      _.isEqual(prevProps.querySource, querySource)
    ) {
      return
    }

    this._getTags()
  },

  render() {
    const {query} = this.props

    return (
      <div className="query-builder--sub-list">
        {_.map(this.state.tags, (tagValues, tagKey) => {
          return (
            <TagListItem
              key={tagKey}
              tagKey={tagKey}
              tagValues={tagValues}
              selectedTagValues={query.tags[tagKey] || []}
              isUsingGroupBy={query.groupBy.tags.indexOf(tagKey) > -1}
              onChooseTag={this.props.onChooseTag}
              onGroupByTag={this.props.onGroupByTag}
            />
          )
        })}
      </div>
    )
  },
})

export default TagList
