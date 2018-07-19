import React, {PureComponent} from 'react'
import _ from 'lodash'

import Tags from 'src/shared/components/Tags'
import {ErrorHandling} from 'src/shared/decorators/errors'

interface Item {
  text?: string
  name?: string
}
interface Props {
  onAddTag: (item: Item) => void
  onDeleteTag: (item: Item) => void
  tags: Item[]
  title: string
  disableTest: () => void
  inputID?: string
}

@ErrorHandling
class TagInput extends PureComponent<Props> {
  private input: HTMLInputElement

  public render() {
    const {title, tags, inputID} = this.props
    const id = inputID || title

    return (
      <div className="form-group col-xs-12">
        <label htmlFor={id}>{title}</label>
        <input
          placeholder={`Type and hit 'Enter' to add to list of ${title}`}
          autoComplete="off"
          className="form-control tag-input"
          id={id}
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

  private handleDeleteTag = (item: Item) => {
    this.props.onDeleteTag(item)
  }

  private shouldAddToList(item: Item, tags: Item[]): boolean {
    return !_.isEmpty(item) && !tags.find(l => l === item)
  }
}

export default TagInput
