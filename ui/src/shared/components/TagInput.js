import React, {Component, PropTypes} from 'react'
import _ from 'lodash'

import Tags from 'shared/components/Tags'

class TagInput extends Component {
  handleAddTag = e => {
    if (e.key === 'Enter') {
      e.preventDefault()
      const newItem = e.target.value.trim()
      const {tags, onAddTag} = this.props
      if (!this.shouldAddToList(newItem, tags)) {
        return
      }

      this.input.value = ''
      onAddTag(newItem)
    }
  }

  shouldAddToList(item, tags) {
    return !_.isEmpty(item) && !tags.find(l => l === item)
  }

  render() {
    const {title, tags, onDeleteTag} = this.props

    return (
      <div className="form-group col-xs-12">
        <label htmlFor={title}>
          {title}
        </label>
        <input
          placeholder={`Type and hit 'Enter' to add to list of ${title}`}
          autoComplete="off"
          className="form-control"
          id={title}
          type="text"
          ref={r => (this.input = r)}
          onKeyDown={this.handleAddTag}
        />
        <Tags tags={tags} onDeleteTag={onDeleteTag} />
      </div>
    )
  }
}

const {arrayOf, func, string} = PropTypes

TagInput.propTypes = {
  onAddTag: func.isRequired,
  onDeleteTag: func.isRequired,
  tags: arrayOf(string).isRequired,
  title: string.isRequired,
}

export default TagInput
