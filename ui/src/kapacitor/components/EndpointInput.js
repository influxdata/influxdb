import React, {PropTypes} from 'react'

const EndpointInput = ({
  fieldName,
  fieldDisplay,
  placeholder,
  selectedEndpoint,
  handleModifyEndpoint,
  redacted = false,
  editable = true,
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

const {func, shape, string, bool} = PropTypes

EndpointInput.propTypes = {
  fieldName: string.isRequired,
  fieldDisplay: string,
  placeholder: string,
  redacted: bool,
  editable: bool,
  selectedEndpoint: shape({}).isRequired,
  handleModifyEndpoint: func.isRequired,
}

export default EndpointInput
