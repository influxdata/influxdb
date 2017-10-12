import React, {PropTypes} from 'react'
import OnClickOutside from 'react-onclickoutside'

const LayoutCellMenu = OnClickOutside(
  ({
    isDeleting,
    onEdit,
    onDeleteClick,
    onDelete,
    onCSVDownload,
    queriesExist,
    cell,
  }) =>
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
      {queriesExist
        ? <div
            className="dash-graph-context--button"
            onClick={onCSVDownload(cell)}
          >
            <span className="icon download" />
          </div>
        : null}
      {isDeleting
        ? <div className="dash-graph-context--button active">
            <span className="icon trash" />
            <div
              className="dash-graph-context--confirm"
              onClick={onDelete(cell)}
            >
              Confirm
            </div>
          </div>
        : <div className="dash-graph-context--button" onClick={onDeleteClick}>
            <span className="icon trash" />
          </div>}
    </div>
)

const LayoutCellMenuContainer = props => {
  if (!props.isEditable) {
    return null
  }

  return <LayoutCellMenu {...props} />
}

const {bool, func, shape} = PropTypes

LayoutCellMenuContainer.propTypes = {
  isDeleting: bool,
  onEdit: func,
  onDelete: func,
  onDeleteClick: func,
  cell: shape(),
  isEditable: bool,
}

LayoutCellMenu.propTypes = LayoutCellMenuContainer.propTypes

export default LayoutCellMenuContainer
