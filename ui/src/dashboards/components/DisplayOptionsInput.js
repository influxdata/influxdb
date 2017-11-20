import React, {PropTypes} from 'react'

const DisplayOptionsInput = ({
  id,
  name,
  value,
  onChange,
  labelText,
  colWidth,
  placeholder,
}) =>
  <div className={`form-group ${colWidth}`}>
    <label htmlFor={name}>
      {labelText}
    </label>
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

const {func, string} = PropTypes

DisplayOptionsInput.defaultProps = {
  value: '',
  colWidth: 'col-sm-6',
  placeholder: '',
}

DisplayOptionsInput.propTypes = {
  name: string.isRequired,
  id: string.isRequired,
  value: string.isRequired,
  onChange: func.isRequired,
  labelText: string,
  colWidth: string,
  placeholder: string,
}

export default DisplayOptionsInput
