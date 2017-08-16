import React, {PropTypes} from 'react'
import TableInput from 'src/dashboards/components/template_variables/TableInput'

const RowValues = ({
  selectedType,
  values = [],
  isEditing,
  onStartEdit,
  autoFocusTarget,
}) => {
  const _values = values.map(v => v.value).join(', ')

  if (selectedType === 'csv') {
    return (
      <TableInput
        name="values"
        defaultValue={_values}
        isEditing={isEditing}
        onStartEdit={onStartEdit}
        autoFocusTarget={autoFocusTarget}
        spellCheck={false}
        autoComplete={false}
      />
    )
  }
  return (
    <div className={values.length ? 'tvm-values' : 'tvm-values-empty'}>
      {values.length ? _values : 'No values to display'}
    </div>
  )
}

const {arrayOf, bool, func, shape, string} = PropTypes

RowValues.propTypes = {
  selectedType: string.isRequired,
  values: arrayOf(shape()),
  isEditing: bool.isRequired,
  onStartEdit: func.isRequired,
  autoFocusTarget: string,
}

export default RowValues
