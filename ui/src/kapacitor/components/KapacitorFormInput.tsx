import React, {SFC, ChangeEvent} from 'react'

interface Props {
  name: string
  label: string
  value: string
  placeholder: string
  onChange: (e: ChangeEvent<HTMLInputElement>) => void
  maxLength?: number
}

const KapacitorFormInput: SFC<Props> = ({
  name,
  label,
  value,
  placeholder,
  onChange,
  maxLength,
}) =>
  <div className="form-group">
    <label htmlFor={name}>
      {label}
    </label>
    <input
      className="form-control"
      id={name}
      name={name}
      value={value}
      spellCheck={false}
      onChange={onChange}
      placeholder={placeholder}
      maxLength={maxLength}
    />
  </div>

export default KapacitorFormInput
