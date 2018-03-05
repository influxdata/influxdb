import React, {PropTypes} from 'react'

import InputClickToEdit from 'shared/components/InputClickToEdit'

const GraphOptionsCustomizableColumn = ({
  originalColumnName,
  newColumnName,
  onColumnRename,
}) => {
  return (
    <div className="column-controls--section">
      <div className="column-controls--label">
        {originalColumnName}
      </div>
      <InputClickToEdit
        value={newColumnName}
        wrapperClass="column-controls-input"
        onUpdate={onColumnRename}
        placeholder="Rename..."
      />
    </div>
  )
}
const {func, string} = PropTypes

GraphOptionsCustomizableColumn.propTypes = {
  originalColumnName: string,
  newColumnName: string,
  onColumnRename: func,
}

export default GraphOptionsCustomizableColumn
