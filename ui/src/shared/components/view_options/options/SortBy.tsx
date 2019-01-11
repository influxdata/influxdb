import _ from 'lodash'
import React from 'react'

// Types
import {Dropdown, Form, DropdownMode} from 'src/clockface'
import {FieldOption} from 'src/types/v2/dashboards'

interface Props {
  selected: FieldOption
  fieldOptions: FieldOption[]
  onChange: (fieldOption: FieldOption) => void
}

const SortBy = ({fieldOptions, onChange, selected}: Props) => {
  return (
    <Form.Element label="Default Sort Field">
      <Dropdown
        selectedID={_.get(selected, 'internalName', null)}
        customClass="dropdown-stretch"
        onChange={onChange}
        mode={DropdownMode.ActionList}
        titleText="Choose a sort field"
      >
        {fieldOptions.map(field => (
          <Dropdown.Item
            key={field.internalName}
            id={field.internalName}
            value={field}
          >
            {field.displayName}
          </Dropdown.Item>
        ))}
      </Dropdown>
    </Form.Element>
  )
}

export default SortBy
