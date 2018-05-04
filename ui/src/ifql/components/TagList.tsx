import PropTypes from 'prop-types'
import React, {PureComponent} from 'react'

import _ from 'lodash'

import TagListItem from 'src/ifql/components/TagListItem'

import {getTags, getTagValues} from 'src/ifql/apis'
import {ErrorHandling} from 'src/shared/decorators/errors'

const {shape} = PropTypes

interface Props {
  db: string
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
    const {db} = this.props
    if (!db) {
      return
    }

    this.getTags()
  }

  public async getTags() {
    const keys = await getTags()
    const values = await getTagValues()

    const tags = keys.map(k => {
      return (this.state.tags[k] = values)
    })

    this.setState({tags})
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
