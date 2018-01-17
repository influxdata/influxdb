import React, {PropTypes} from 'react'

const Tags = ({tags, onDeleteTag}) =>
  <div className="input-tag-list">
    {tags.map(item => {
      return <Tag key={item} item={item} onDelete={onDeleteTag} />
    })}
  </div>

const Tag = ({item, onDelete}) =>
  <span key={item} className="input-tag-item">
    <span>
      {item}
    </span>
    <span className="icon remove" onClick={onDelete(item)} />
  </span>

const {arrayOf, func, string} = PropTypes

Tags.propTypes = {
  tags: arrayOf(string),
  onDeleteTag: func,
}

Tag.propTypes = {
  item: string,
  onDelete: func,
}

export default Tags
