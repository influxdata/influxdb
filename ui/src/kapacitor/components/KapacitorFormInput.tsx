import React, {ChangeEvent, SFC} from 'react'

interface Props {
  name: string
  label: string
  value: string
  placeholder: string
  maxLength?: number
  inputType?: string
  customClass?: string
  onChange: (e: ChangeEvent<HTMLInputElement>) => void
}

const KapacitorFormInput: SFC<Props> = ({
  name,
  label,
  value,
  placeholder,
  onChange,
  maxLength,
  inputType,
  customClass,
}) => (
  <div className={`form-group ${customClass}`}>
    <label htmlFor={name}>{label}</label>
    <input
      className="form-control"
      id={name}
      name={name}
      value={value}
      spellCheck={false}
      onChange={onChange}
      placeholder={placeholder}
      maxLength={maxLength}
      type={inputType}
    />
  </div>
)

KapacitorFormInput.defaultProps = {
  inputType: '',
}

export default KapacitorFormInput
