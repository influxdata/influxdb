import React, {PureComponent, SFC} from 'react'
import TagsAddButton from 'src/shared/components/TagsAddButton'
import ConfirmButton from 'src/shared/components/ConfirmButton'
import uuid from 'uuid'
import {ErrorHandling} from 'src/shared/decorators/errors'

interface Item {
  text?: string
  name?: string
}

interface TagsProps {
  tags: Item[]
  onDeleteTag?: (item: Item) => void
  addMenuItems?: Item[]
  addMenuChoose?: (item: Item) => void
}

const Tags: SFC<TagsProps> = ({
  tags,
  onDeleteTag,
  addMenuItems,
  addMenuChoose,
}) => {
  return (
    <div className="input-tag-list">
      {tags.map(item => {
        return <Tag key={uuid.v4()} item={item} onDelete={onDeleteTag} />
      })}
      {addMenuItems && addMenuItems.length && addMenuChoose ? (
        <TagsAddButton items={addMenuItems} onChoose={addMenuChoose} />
      ) : null}
    </div>
  )
}

interface TagProps {
  item: Item
  onDelete: (item: Item) => void
}

@ErrorHandling
class Tag extends PureComponent<TagProps> {
  public render() {
    const {item} = this.props
    return (
      <span key={uuid.v4()} className="input-tag--item">
        <span>{item.text || item.name || item}</span>
        <ConfirmButton
          icon="remove"
          size="btn-xs"
          customClass="input-tag--remove"
          square={true}
          confirmText="Remove user from organization?"
          confirmAction={this.handleClickDelete(item)}
        />
      </span>
    )
  }

  private handleClickDelete = item => () => {
    this.props.onDelete(item)
  }
}

export default Tags
