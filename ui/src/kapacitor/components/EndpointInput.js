import React, {PropTypes} from 'react'

const EndpointInput = ({
  fieldName,
  fieldDisplay,
  placeholder,
  selectedEndpoint,
  handleModifyEndpoint,
}) => {
  return (
    <div className="form-group">
      <label htmlFor={fieldName}>
        {fieldDisplay}
      </label>
      <input
        name={fieldName}
        id={fieldName}
        className="form-control input-sm form-malachite"
        type="text"
        placeholder={placeholder}
        onChange={handleModifyEndpoint(selectedEndpoint, fieldName)}
        value={selectedEndpoint[fieldName]}
        autoComplete="off"
        spellCheck="false"
      />
    </div>
  )
}

const {func, shape, string} = PropTypes

EndpointInput.propTypes = {
  fieldName: string,
  fieldDisplay: string,
  placeholder: string,
  selectedEndpoint: shape({}).isRequired,
  handleModifyEndpoint: func.isRequired,
}

export default EndpointInput
