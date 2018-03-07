import React, {PropTypes} from 'react'

import InputClickToEdit from 'shared/components/InputClickToEdit'

const GraphOptionsCustomizableColumn = ({
  originalColumnName,
  newColumnName,
  onColumnRename,
}) => {
  return (
    <div className="gauge-controls--section">
      <div className="gauge-controls--label">
        {originalColumnName}
      </div>
      <InputClickToEdit
        value={newColumnName}
        wrapperClass="fancytable--td orgs-table--name"
        onBlur={onColumnRename}
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
