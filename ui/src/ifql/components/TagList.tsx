import PropTypes from 'prop-types'
import React, {PureComponent} from 'react'

import _ from 'lodash'

import TagListItem from 'src/ifql/components/TagListItem'

import {showTagKeys, showTagValues} from 'src/shared/apis/metaQuery'
import showTagKeysParser from 'src/shared/parsing/showTagKeys'
import showTagValuesParser from 'src/shared/parsing/showTagValues'
import {ErrorHandling} from 'src/shared/decorators/errors'

const {shape} = PropTypes

interface Props {
  db: string
  measurement: string
}

interface State {
  tags: {}
  selectedTag: string
}

@ErrorHandling
class TagList extends PureComponent<Props, State> {
  public static contextTypes = {
    source: shape({
      links: shape({}).isRequired,
    }).isRequired,
  }

  constructor(props) {
    super(props)
    this.state = {
      tags: {},
      selectedTag: '',
    }
  }

  public componentDidMount() {
    const {db, measurement} = this.props
    if (!db || !measurement) {
      return
    }

    this.getTags()
  }

  public componentDidUpdate(prevProps) {
    const {db, measurement} = this.props

    const {db: prevDB, measurement: prevMeas} = prevProps

    if (!db || !measurement) {
      return
    }

    if (db === prevDB && measurement === prevMeas) {
      return
    }

    this.getTags()
  }

  public async getTags() {
    const {db, measurement} = this.props
    const {source} = this.context

    const {data} = await showTagKeys({
      database: db,
      measurement,
      retentionPolicy: 'autogen',
      source: source.links.proxy,
    })
    const {tagKeys} = showTagKeysParser(data)

    const response = await showTagValues({
      database: db,
      measurement,
      retentionPolicy: 'autogen',
      source: source.links.proxy,
      tagKeys,
    })

    const {tags} = showTagValuesParser(response.data)

    const selected = Object.keys(tags)
    this.setState({tags, selectedTag: selected[0]})
  }

  public render() {
    return (
      <div className="query-builder--sub-list">
        {_.map(this.state.tags, (tagValues: string[], tagKey: string) => (
          <TagListItem key={tagKey} tagKey={tagKey} tagValues={tagValues} />
        ))}
      </div>
    )
  }
}

export default TagList
