import React, {Component, PropTypes} from 'react'
import TagsAddButton from 'src/shared/components/TagsAddButton'

const Tags = ({tags, onDeleteTag, addMenuItems, addMenuChoose}) =>
  <div className="input-tag-list">
    {tags.map(item => {
      return (
        <Tag
          key={item.text || item.name || item}
          item={item}
          onDelete={onDeleteTag}
        />
      )
    })}
    {addMenuItems.length && addMenuChoose
      ? <TagsAddButton items={addMenuItems} onChoose={addMenuChoose} />
      : null}
  </div>

class Tag extends Component {
  handleClickDelete = item => () => {
    this.props.onDelete(item)
  }

  render() {
    const {item} = this.props
    return (
      <span key={item} className="input-tag-item">
        <span>
          {item.text || item.name || item}
        </span>
        <span className="icon remove" onClick={this.handleClickDelete(item)} />
      </span>
    )
  }
}

const {arrayOf, func, oneOfType, shape, string} = PropTypes

Tags.propTypes = {
  tags: arrayOf(oneOfType([shape(), string])),
  onDeleteTag: func,
  addMenuItems: arrayOf(shape({})),
  addMenuChoose: func,
}

Tag.propTypes = {
  item: oneOfType([shape(), string]),
  onDelete: func,
}

export default Tags
