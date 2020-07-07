// Libraries
import React, {FC} from 'react'
import {
  Button,
  ButtonShape,
  Form,
  IconFont,
  SelectDropdown,
} from '@influxdata/clockface'
import {connect, ConnectedProps} from 'react-redux'

// Components
import SearchableDropdown from 'src/shared/components/SearchableDropdown'

// Types
import {Filter} from 'src/types'

// Actions
import {setValuesByKey} from 'src/shared/actions/predicates'

interface OwnProps {
  bucket: string
  filter: Filter
  keys: string[]
  onChange: (filter: Filter) => any
  onDelete: () => any
  shouldValidate: boolean
  values: (string | number)[]
}

type ReduxProps = ConnectedProps<typeof connector>
type Props = OwnProps & ReduxProps

const FilterRow: FC<Props> = ({
  bucket,
  filter: {key, equality, value},
  keys,
  onChange,
  onDelete,
  setValuesByKey,
  shouldValidate,
  values,
}) => {
  const keyErrorMessage =
    shouldValidate && key.trim() === '' ? 'Key cannot be empty' : null
  const equalityErrorMessage =
    shouldValidate && equality.trim() === '' ? 'Equality cannot be empty' : null
  const valueErrorMessage =
    shouldValidate && value.trim() === '' ? 'Value cannot be empty' : null

  const onChangeKey = (input: string) => onChange({key: input, equality, value})
  const onKeySelect = (input: string) => {
    setValuesByKey(bucket, input)
    onChange({key: input, equality, value})
  }
  const onChangeValue = (input: string) =>
    onChange({key, equality, value: input})

  const onChangeEquality = (e: string) => onChange({key, equality: e, value})

  return (
    <div className="delete-data-filter">
      <Form.Element
        label="Tag Key"
        required={true}
        errorMessage={keyErrorMessage}
      >
        <SearchableDropdown
          className="dwp-filter-dropdown"
          searchTerm={key}
          emptyText="No Tags Found"
          searchPlaceholder="Search keys..."
          selectedOption={key}
          onSelect={onKeySelect}
          onChangeSearchTerm={onChangeKey}
          testID="dwp-filter-key-input"
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
          className="dwp-filter-dropdown"
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
          className="dwp-filter-dropdown"
          searchTerm={value}
          emptyText="No Tags Found"
          searchPlaceholder="Search values..."
          selectedOption={value}
          onSelect={onChangeValue}
          onChangeSearchTerm={onChangeValue}
          testID="dwp-filter-value-input"
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

const mdtp = {setValuesByKey}

const connector = connect(null, mdtp)

export default connector(FilterRow)
