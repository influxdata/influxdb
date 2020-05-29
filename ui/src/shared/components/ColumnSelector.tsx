// Libraries
import React, {FunctionComponent} from 'react'

// Components
import {SelectDropdown, Form, ComponentStatus} from '@influxdata/clockface'

interface Props {
  selectedColumn: string
  availableColumns: string[]
  axisName: string
  onSelectColumn: (col: string) => void
}

const ColumnSelector: FunctionComponent<Props> = ({
  selectedColumn,
  onSelectColumn,
  availableColumns,
  axisName,
}) => {
  return (
    <Form.Element label={`${axisName.toUpperCase()} Column`}>
      <SelectDropdown
        options={availableColumns}
        selectedOption={selectedColumn || 'Build a query before selecting...'}
        onSelect={onSelectColumn}
        buttonStatus={
          availableColumns.length == 0
            ? ComponentStatus.Disabled
            : ComponentStatus.Default
        }
      />
    </Form.Element>
  )
}

export default ColumnSelector
