// Libraries
import _ from 'lodash'
import React from 'react'

// Component
import {Form} from '@influxdata/clockface'
import {Grid, Dropdown} from 'src/clockface'

// Types
import {DropdownMode} from 'src/clockface'
import {FieldOption} from 'src/types/dashboards'

interface Props {
  selected: FieldOption
  fieldOptions: FieldOption[]
  onChange: (fieldOption: FieldOption) => void
}

const SortBy = ({fieldOptions, onChange, selected}: Props) => {
  return (
    <Grid.Column>
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
    </Grid.Column>
  )
}

export default SortBy
