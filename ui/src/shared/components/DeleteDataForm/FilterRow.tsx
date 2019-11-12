// Libraries
import React, {FC} from 'react'
import {
  Button,
  ButtonShape,
  Form,
  IconFont,
  SelectDropdown,
} from '@influxdata/clockface'

// Components
import SearchableDropdown from 'src/shared/components/SearchableDropdown'

// Types
import {Filter} from 'src/types'

interface Props {
  filter: Filter
  keys: string[]
  onChange: (filter: Filter) => any
  onDelete: () => any
  shouldValidate: boolean
  values: (string | number)[]
}

const FilterRow: FC<Props> = ({
  filter: {key, equality, value},
  keys,
  onChange,
  onDelete,
  shouldValidate,
  values,
}) => {
  const keyErrorMessage =
    shouldValidate && key.trim() === '' ? 'Key cannot be empty' : null
  const equalityErrorMessage =
    shouldValidate && equality.trim() === '' ? 'Equality cannot be empty' : null
  const valueErrorMessage =
    shouldValidate && value.trim() === '' ? 'Value cannot be empty' : null

  const onChangeKey = input => onChange({key: input, equality, value})
  const onChangeValue = input => onChange({key, equality, value: input})
  const onChangeEquality = e => onChange({key, equality: e, value})

  return (
    <div className="delete-data-filter">
      <Form.Element
        label="Tag Key"
        required={true}
        errorMessage={keyErrorMessage}
      >
        <SearchableDropdown
          searchTerm={key}
          emptyText="No Tags Found"
          searchPlaceholder="Search keys..."
          selectedOption={key}
          onSelect={onChangeKey}
          onChangeSearchTerm={onChangeKey}
          testID="key-input"
          buttonTestID="tag-selector--dropdown-button"
          menuTestID="tag-selector--dropdown-menu"
          options={keys}
        />
      </Form.Element>
      <Form.Element
        label="Equality Filter"
        required={true}
        errorMessage={equalityErrorMessage}
      >
        <SelectDropdown
          options={['=', '!=']}
          selectedOption={equality}
          onSelect={onChangeEquality}
        />
      </Form.Element>
      <Form.Element
        label="Tag Value"
        required={true}
        errorMessage={valueErrorMessage}
      >
        <SearchableDropdown
          searchTerm={value}
          emptyText="No Tags Found"
          searchPlaceholder="Search values..."
          selectedOption={value}
          onSelect={onChangeValue}
          onChangeSearchTerm={onChangeValue}
          testID="value-input"
          buttonTestID="tag-selector--dropdown-button"
          menuTestID="tag-selector--dropdown-menu"
          options={values}
        />
      </Form.Element>
      <Button
        className="delete-data-filter--remove"
        shape={ButtonShape.Square}
        icon={IconFont.Remove}
        onClick={onDelete}
      />
    </div>
  )
}

export default FilterRow
