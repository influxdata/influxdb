import React, {PropTypes} from 'react'
import DeleteConfirmButtons from 'shared/components/DeleteConfirmButtons'

const RowButtons = ({
  onStartEdit,
  isEditing,
  onCancelEdit,
  onDelete,
  id,
  selectedType,
}) => {
  if (isEditing) {
    return (
      <div className="tvm-actions">
        <button
          className="btn btn-sm btn-info"
          type="button"
          onClick={onCancelEdit}
        >
          Cancel
        </button>
        <button className="btn btn-sm btn-success" type="submit">
          {selectedType === 'csv' ? 'Save Values' : 'Get Values'}
        </button>
      </div>
    )
  }
  return (
    <div className="tvm-actions">
      <DeleteConfirmButtons onDelete={onDelete(id)} />
      <button
        className="btn btn-sm btn-info btn-edit btn-square"
        type="button"
        onClick={onStartEdit('tempVar')}
      >
        <span className="icon pencil" />
      </button>
    </div>
  )
}

const {bool, func, string} = PropTypes

RowButtons.propTypes = {
  onStartEdit: func.isRequired,
  isEditing: bool.isRequired,
  onCancelEdit: func.isRequired,
  onDelete: func.isRequired,
  id: string.isRequired,
  selectedType: string.isRequired,
}

export default RowButtons
