import React from 'react'
import PropTypes from 'prop-types'

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
        onBlur={onColumnRename}
        placeholder="Rename..."
        appearAsNormalInput={true}
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
