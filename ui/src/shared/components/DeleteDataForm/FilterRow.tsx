// Libraries
import React, {FunctionComponent} from 'react'
import {Form, Input, Button, ButtonShape, IconFont} from '@influxdata/clockface'

// Types
import {Filter} from 'src/shared/components/DeleteDataForm/FilterEditor'

interface Props {
  filter: Filter
  onChange: (filter: Filter) => any
  onDelete: () => any
  shouldValidate: boolean
}

const FilterRow: FunctionComponent<Props> = ({
  filter: {key, value},
  onChange,
  onDelete,
  shouldValidate,
}) => {
  const keyErrorMessage =
    shouldValidate && key.trim() === '' ? 'Key cannot be empty' : null

  const valueErrorMessage =
    shouldValidate && value.trim() === '' ? 'Value cannot be empty' : null

  const onChangeKey = e => onChange({key: e.target.value, value})
  const onChangeValue = e => onChange({key, value: e.target.value})

  return (
    <div className="delete-data-filter">
      <Form.Element
        label="Tag Key"
        required={true}
        errorMessage={keyErrorMessage}
      >
        <Input value={key} onChange={onChangeKey} />
      </Form.Element>
      <div className="delete-data-filter--equals">==</div>
      <Form.Element
        label="Tag Value"
        required={true}
        errorMessage={valueErrorMessage}
      >
        <Input value={value} onChange={onChangeValue} />
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
