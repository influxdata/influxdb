// Libraries
import React, {FunctionComponent} from 'react'

// Components
import {Dropdown, Form, ComponentStatus} from 'src/clockface'

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
      <Dropdown
        selectedID={selectedColumn}
        onChange={onSelectColumn}
        status={
          availableColumns.length == 0
            ? ComponentStatus.Disabled
            : ComponentStatus.Default
        }
        titleText="None"
      >
        {availableColumns.map(columnName => (
          <Dropdown.Item id={columnName} key={columnName} value={columnName}>
            {columnName}
          </Dropdown.Item>
        ))}
      </Dropdown>
    </Form.Element>
  )
}

export default ColumnSelector
