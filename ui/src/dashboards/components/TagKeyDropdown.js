import React, {PropTypes, Component} from 'react'

import Dropdown from 'shared/components/Dropdown'
import {showTagKeys} from 'shared/apis/metaQuery'
import parsers from 'shared/parsing'
const {tagKeys: showTagKeysParser} = parsers

class TagKeyDropdown extends Component {
  constructor(props) {
    super(props)
    this.state = {
      tagKeys: [],
    }
  }

  componentDidMount() {
    this._getTags()
  }

  componentDidUpdate(nextProps) {
    if (
      nextProps.database === this.props.database &&
      nextProps.measurement === this.props.measurement
    ) {
      return
    }

    this._getTags()
  }

  render() {
    const {tagKeys} = this.state
    const {tagKey, onSelectTagKey} = this.props
    return (
      <Dropdown
        items={tagKeys.map(text => ({text}))}
        selected={tagKey || 'Select Tag Key'}
        onChoose={onSelectTagKey}
        onClick={this.handleStartEdit}
      />
    )
  }

  handleStartEdit = () => this.props.onStartEdit(null)

  _getTags = async () => {
    const {
      database,
      measurement,
      tagKey,
      onSelectTagKey,
      onErrorThrown,
    } = this.props
    const {source: {links: {proxy}}} = this.context

    try {
      const {data} = await showTagKeys({source: proxy, database, measurement})
      const {tagKeys} = showTagKeysParser(data)

      this.setState({tagKeys})
      const selectedTagKeyText = tagKeys.includes(tagKey)
        ? tagKey
        : tagKeys[0] || 'No tags'
      onSelectTagKey({text: selectedTagKeyText})
    } catch (error) {
      console.error(error)
      onErrorThrown(error)
    }
  }
}

const {func, shape, string} = PropTypes

TagKeyDropdown.contextTypes = {
  source: shape({
    links: shape({
      proxy: string.isRequired,
    }).isRequired,
  }).isRequired,
}

TagKeyDropdown.propTypes = {
  database: string.isRequired,
  measurement: string.isRequired,
  tagKey: string,
  onSelectTagKey: func.isRequired,
  onStartEdit: func.isRequired,
  onErrorThrown: func.isRequired,
}

export default TagKeyDropdown
