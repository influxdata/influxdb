import React, {PureComponent} from 'react'
import PropTypes from 'prop-types'
import _ from 'lodash'

import TagListItem from 'src/shared/components/TagListItem'

import {Query, Source} from 'src/types'

import {showTagKeys, showTagValues} from 'src/shared/apis/metaQuery'
import showTagKeysParser from 'src/shared/parsing/showTagKeys'
import showTagValuesParser from 'src/shared/parsing/showTagValues'

const {string, shape} = PropTypes

interface Props {
  query: Query
  querySource: Source
  onChooseTag: () => void
  onGroupByTag: () => void
}

interface State {
  tags: {}
}

class TagList extends PureComponent<Props, State> {
  constructor(props) {
    super(props)
    this.state = {
      tags: {},
    }
  }

  public static contextTypes = {
    source: shape({
      links: shape({
        proxy: string.isRequired,
      }).isRequired,
    }).isRequired,
  }

  public static defaultProps = {
    querySource: null,
  }

  componentDidMount() {
    const {database, measurement, retentionPolicy} = this.props.query
    if (!database || !measurement || !retentionPolicy) {
      return
    }

    this.getTags()
  }

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

    this.getTags()
  }

  async getTags() {
    const {source} = this.context
    const {querySource} = this.props
    const {database, measurement, retentionPolicy} = this.props.query

    const proxy = _.get(querySource, ['links', 'proxy'], source.links.proxy)

    const {data} = await showTagKeys({
      source: proxy,
      database,
      retentionPolicy,
      measurement,
    })
    const {tagKeys} = showTagKeysParser(data)

    const response = await showTagValues({
      source: proxy,
      database,
      retentionPolicy,
      measurement,
      tagKeys,
    })

    const {tags} = showTagValuesParser(response.data)

    this.setState({tags})
  }

  render() {
    const {query} = this.props

    return (
      <div className="query-builder--sub-list">
        {_.map(this.state.tags, (tagValues: string[], tagKey: string) => {
          return (
            <TagListItem
              key={tagKey}
              tagKey={tagKey}
              tagValues={tagValues}
              isUsingGroupBy={query.groupBy.tags.indexOf(tagKey) > -1}
              selectedTagValues={query.tags[tagKey] || []}
              onChooseTag={this.props.onChooseTag}
              onGroupByTag={this.props.onGroupByTag}
            />
          )
        })}
      </div>
    )
  }
}

export default TagList
