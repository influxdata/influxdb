// Libraries
import React, {FC} from 'react'
import {
  Button,
  ButtonShape,
  Form,
  IconFont,
  Input,
  SelectDropdown,
} from '@influxdata/clockface'
import {FeatureFlag} from 'src/shared/utils/featureFlag'

// Types
import {Filter} from 'src/types'

interface Props {
  filter: Filter
  onChange: (filter: Filter) => any
  onDelete: () => any
  shouldValidate: boolean
}

const FilterRow: FC<Props> = ({
  filter: {key, equality, value},
  onChange,
  onDelete,
  shouldValidate,
}) => {
  const keyErrorMessage =
    shouldValidate && key.trim() === '' ? 'Key cannot be empty' : null
  const equalityErrorMessage =
    shouldValidate && equality.trim() === '' ? 'Equality cannot be empty' : null
  const valueErrorMessage =
    shouldValidate && value.trim() === '' ? 'Value cannot be empty' : null

  const onChangeKey = e => onChange({key: e.target.value, equality, value})
  const onChangeValue = e => onChange({key, equality, value: e.target.value})
  const onChangeEquality = e => onChange({key, equality: e, value})

  return (
    <div className="delete-data-filter">
      <Form.Element
        label="Tag Key"
        required={true}
        errorMessage={keyErrorMessage}
      >
        <Input onChange={onChangeKey} value={key} testID="key-input" />
      </Form.Element>
      <div className="delete-data-filter--equals">==</div>
      <FeatureFlag name="deleteWithPredicate">
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
      </FeatureFlag>
      <Form.Element
        label="Tag Value"
        required={true}
        errorMessage={valueErrorMessage}
      >
        <Input onChange={onChangeValue} value={value} testID="value-input" />
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
