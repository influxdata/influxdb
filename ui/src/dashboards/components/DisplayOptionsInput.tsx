import React, {SFC, ChangeEvent} from 'react'

interface Props {
  name: string
  id: string
  value: string
  onChange: (e: ChangeEvent<HTMLInputElement>) => void
  labelText?: string
  colWidth?: string
  placeholder?: string
}

const DisplayOptionsInput: SFC<Props> = ({
  id,
  name,
  value,
  onChange,
  labelText,
  colWidth,
  placeholder,
}) => (
  <div className={`form-group ${colWidth}`}>
    <label htmlFor={name}>{labelText}</label>
    <input
      className="form-control input-sm"
      type="text"
      name={name}
      id={id}
      value={value}
      onChange={onChange}
      placeholder={placeholder}
    />
  </div>
)

DisplayOptionsInput.defaultProps = {
  value: '',
  colWidth: 'col-sm-6',
  placeholder: '',
}

export default DisplayOptionsInput
