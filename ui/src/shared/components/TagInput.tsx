// Libraries
import React, {PureComponent} from 'react'
import _ from 'lodash'

// Components
import Tags from 'src/shared/components/Tags'
import {ErrorHandling} from 'src/shared/decorators/errors'

export interface Item {
  text?: string
  name?: string
}
interface Props {
  onAddTag: (item: string) => void
  onDeleteTag: (item: string) => void
  tags: Item[]
  title: string
  displayTitle: boolean
  inputID?: string
  autoFocus?: boolean
}

@ErrorHandling
class TagInput extends PureComponent<Props> {
  private input: HTMLInputElement

  public render() {
    const {title, tags, autoFocus} = this.props

    return (
      <div className="form-group col-xs-12">
        {this.label}
        <input
          placeholder={`Type and hit 'Enter' to add to list of ${title}`}
          autoComplete="off"
          className="form-control tag-input"
          id={this.id}
          type="text"
          ref={r => (this.input = r)}
          onKeyDown={this.handleAddTag}
          autoFocus={autoFocus || false}
        />
        <Tags tags={tags} onDeleteTag={this.handleDeleteTag} />
      </div>
    )
  }

  private get id(): string {
    const {title, inputID} = this.props
    return inputID || title
  }

  private get label(): JSX.Element {
    const {title, displayTitle} = this.props

    if (displayTitle) {
      return <label htmlFor={this.id}>{title}</label>
    }
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
    }
  }

  private handleDeleteTag = (item: Item) => {
    this.props.onDeleteTag(item.name || item.text)
  }

  private shouldAddToList(item: Item, tags: Item[]): boolean {
    return !_.isEmpty(item) && !tags.find(l => l === item)
  }
}

export default TagInput
