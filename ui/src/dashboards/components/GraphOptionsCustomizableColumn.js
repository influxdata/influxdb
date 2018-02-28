import React, {PropTypes} from 'react'

import InputClickToEdit from 'shared/components/InputClickToEdit'

const GraphOptionsCustomizableColumn = ({
  originalColumnName,
  newColumnName,
  handleColumnRename,
}) => {
  return (
    <div className="gauge-controls--section">
      <div className="gauge-controls--label">
        {originalColumnName}
      </div>
      <InputClickToEdit
        value={newColumnName}
        wrapperClass="fancytable--td orgs-table--name"
        onUpdate={handleColumnRename}
        placeholder="Rename..."
      />
    </div>
  )
}
const {func, string} = PropTypes

GraphOptionsCustomizableColumn.propTypes = {
  originalColumnName: string.isRequired,
  newColumnName: string.isRequired,
  handleColumnRename: func.isRequired,
}

export default GraphOptionsCustomizableColumn
