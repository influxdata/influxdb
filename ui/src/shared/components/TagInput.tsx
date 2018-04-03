import React, {PureComponent} from 'react'
import _ from 'lodash'

import Tags from 'src/shared/components/Tags'
import {Input} from 'src/types/kapacitor'

interface Props {
  onAddTag: (item: string) => void
  onDeleteTag: (item: string) => void
  tags: string[]
  title: string
  disableTest: () => void
}

class TagInput extends PureComponent<Props> {
  private input: Input

  public render() {
    const {title, tags} = this.props

    return (
      <div className="form-group col-xs-12">
        <label htmlFor={title}>{title}</label>
        <input
          placeholder={`Type and hit 'Enter' to add to list of ${title}`}
          autoComplete="off"
          className="form-control tag-input"
          id={title}
          type="text"
          ref={r => (this.input = r)}
          onKeyDown={this.handleAddTag}
        />
        <Tags tags={tags} onDeleteTag={this.handleDeleteTag} />
      </div>
    )
  }

  private handleAddTag = e => {
    if (e.key === 'Enter') {
      e.preventDefault()
      const newItem = e.target.value.trim()
      const {tags, onAddTag} = this.props
      if (!this.shouldAddToList(newItem, tags)) {
        return
      }

      this.input.value = ''
      onAddTag(newItem)
      this.props.disableTest()
    }
  }

  private handleDeleteTag = item => {
    this.props.onDeleteTag(item)
  }

  private shouldAddToList(item, tags) {
    return !_.isEmpty(item) && !tags.find(l => l === item)
  }
}

export default TagInput
