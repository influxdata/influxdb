import React, {PropTypes} from 'react'
import OnClickOutside from 'react-onclickoutside'

const ContextMenu = OnClickOutside(
  ({isDeleting, onEdit, onDeleteClick, onDelete, cell}) =>
    <div
      className={
        isDeleting
          ? 'dash-graph-context dash-graph-context__deleting'
          : 'dash-graph-context'
      }
    >
      <div className="dash-graph-context--button" onClick={onEdit(cell)}>
        <span className="icon pencil" />
      </div>
      {isDeleting
        ? <div className="dash-graph-context--confirm" onClick={onDelete(cell)}>
            Confirm
          </div>
        : <div className="dash-graph-context--button" onClick={onDeleteClick}>
            <span className="icon trash" />
          </div>}
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
  isDeleting: bool,
  onEdit: func,
  onDelete: func,
  onDeleteClick: func,
  cell: shape(),
  isEditable: bool,
}

ContextMenu.propTypes = ContextMenuContainer.propTypes

export default ContextMenuContainer
