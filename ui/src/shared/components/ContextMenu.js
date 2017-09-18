import React, {PropTypes} from 'react'
import classnames from 'classnames'
import OnClickOutside from 'react-onclickoutside'

const ContextMenu = OnClickOutside(
  ({
    isOpen,
    isDeleting,
    toggleMenu,
    onEdit,
    onRename,
    onDelete,
    onDeleteClick,
    cell,
  }) =>
    <div
      className={classnames('dash-graph--options', {
        'dash-graph--options-show': isOpen || isDeleting,
      })}
      onClick={toggleMenu}
    >
      <button className="btn btn-info btn-xs">
        <span className="icon caret-down" />
      </button>
      <ul className="dash-graph--options-menu">
        <li onClick={onEdit(cell)}>Edit</li>
        <li onClick={onRename(cell.x, cell.y, cell.isEditing)}>Rename</li>
        {isDeleting
          ? <li className="dash-graph--confirm-delete" onClick={onDelete(cell)}>
              Confirm Delete
            </li>
          : <li onClick={onDeleteClick}>Delete</li>}
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
  isDeleting: bool,
  toggleMenu: func,
  onEdit: func,
  onRename: func,
  onDelete: func,
  onDeleteClick: func,
  cell: shape(),
  isEditable: bool,
}

ContextMenu.propTypes = ContextMenuContainer.propTypes

export default ContextMenuContainer
