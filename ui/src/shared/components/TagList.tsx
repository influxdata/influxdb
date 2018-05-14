import PropTypes from 'prop-types'
import React, {PureComponent} from 'react'

import _ from 'lodash'

import TagListItem from 'src/shared/components/TagListItem'

import {showTagKeys, showTagValues} from 'src/shared/apis/metaQuery'
import showTagKeysParser from 'src/shared/parsing/showTagKeys'
import showTagValuesParser from 'src/shared/parsing/showTagValues'
import {ErrorHandling} from 'src/shared/decorators/errors'
import {QueryConfig, Tag} from 'src/types'

const {shape} = PropTypes

interface SourceLinks {
  proxy: string
}

interface Source {
  links: SourceLinks
}

interface Props {
  query: QueryConfig
  querySource: Source
  onChooseTag: (tag: Tag) => void
  onGroupByTag: (tagKey: string) => void
  isQuerySupportedByExplorer: boolean
}

interface State {
  tags: {}
}

@ErrorHandling
class TagList extends PureComponent<Props, State> {
  public static contextTypes = {
    source: shape({
      links: shape({}).isRequired,
    }).isRequired,
  }

  public static defaultProps = {
    querySource: null,
  }

  constructor(props) {
    super(props)
    this.state = {
      tags: {},
    }
  }

  public componentDidMount() {
    const {database, measurement, retentionPolicy} = this.props.query
    if (!database || !measurement || !retentionPolicy) {
      return
    }

    this.getTags()
  }

  public componentDidUpdate(prevProps) {
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

  public async getTags() {
    const {source} = this.context
    const {querySource} = this.props
    const {database, measurement, retentionPolicy} = this.props.query

    const proxy = _.get(querySource, ['links', 'proxy'], source.links.proxy)

    const {data} = await showTagKeys({
      database,
      measurement,
      retentionPolicy,
      source: proxy,
    })
    const {tagKeys} = showTagKeysParser(data)

    const response = await showTagValues({
      database,
      measurement,
      retentionPolicy,
      source: proxy,
      tagKeys,
    })

    const {tags} = showTagValuesParser(response.data)

    this.setState({tags})
  }

  public render() {
    const {
      query,
      onChooseTag,
      onGroupByTag,
      isQuerySupportedByExplorer,
    } = this.props

    return (
      <div className="query-builder--sub-list">
        {_.map(this.state.tags, (tagValues: string[], tagKey: string) => (
          <TagListItem
            key={tagKey}
            tagKey={tagKey}
            tagValues={tagValues}
            onChooseTag={onChooseTag}
            onGroupByTag={onGroupByTag}
            selectedTagValues={query.tags[tagKey] || []}
            isUsingGroupBy={query.groupBy.tags.indexOf(tagKey) > -1}
            isQuerySupportedByExplorer={isQuerySupportedByExplorer}
          />
        ))}
      </div>
    )
  }
}

export default TagList
