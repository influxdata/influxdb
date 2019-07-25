// Libraries
import _ from 'lodash'
import React from 'react'

// Component
import {Form, Grid, Dropdown} from '@influxdata/clockface'

// Types
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
          className="dropdown-stretch"
          button={(active, onClick) => (
            <Dropdown.Button active={active} onClick={onClick}>
              {_.get(selected, 'displayName', 'Choose a sort field')}
            </Dropdown.Button>
          )}
          menu={onCollapse => (
            <Dropdown.Menu onCollapse={onCollapse}>
              {fieldOptions
                .filter(field => !!field.internalName)
                .map(field => (
                  <Dropdown.Item
                    key={field.internalName}
                    id={field.internalName}
                    value={field}
                    onClick={onChange}
                    selected={
                      field.internalName ===
                      _.get(selected, 'internalName', null)
                    }
                  >
                    {field.displayName}
                  </Dropdown.Item>
                ))}
            </Dropdown.Menu>
          )}
        />
      </Form.Element>
    </Grid.Column>
  )
}

export default SortBy
