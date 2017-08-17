import React, {PropTypes} from 'react'
import classnames from 'classnames'
import OnClickOutside from 'react-onclickoutside'

const ContextMenu = OnClickOutside(
  ({isOpen, toggleMenu, onEdit, onRename, onDelete, cell}) =>
    <div
      className={classnames('dash-graph--options', {
        'dash-graph--options-show': isOpen,
      })}
      onClick={toggleMenu}
    >
      <button className="btn btn-info btn-xs">
        <span className="icon caret-down" />
      </button>
      <ul className="dash-graph--options-menu">
        <li onClick={onEdit(cell)}>Edit</li>
        <li onClick={onRename(cell.x, cell.y, cell.isEditing)}>Rename</li>
        <li onClick={onDelete(cell)}>Delete</li>
      </ul>
    </div>
)

const ContextMenuContainer = props => {
  if (!props.isEditable) {
    return null
  }

  return <ContextMenu {...props} />
}

const {bool, func, shape} = PropTypes

ContextMenuContainer.propTypes = {
  isOpen: bool,
  toggleMenu: func,
  onEdit: func,
  onRename: func,
  onDelete: func,
  cell: shape(),
  isEditable: bool,
}

ContextMenu.propTypes = ContextMenuContainer.propTypes

export default ContextMenuContainer
