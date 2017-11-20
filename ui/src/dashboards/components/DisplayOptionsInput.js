import React, {PropTypes} from 'react'

const DisplayOptionsInput = ({
  id,
  name,
  value,
  onChange,
  labelText,
  colWidth,
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
    />
  </div>

const {func, string} = PropTypes

DisplayOptionsInput.defaultProps = {
  value: '',
  colWidth: 'col-sm-6',
}

DisplayOptionsInput.propTypes = {
  name: string.isRequired,
  id: string.isRequired,
  value: string.isRequired,
  onChange: func.isRequired,
  labelText: string,
  colWidth: string,
}

export default DisplayOptionsInput
