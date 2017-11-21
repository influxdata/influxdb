import React, {PropTypes} from 'react'

const HandlerInput = ({
  fieldName,
  fieldDisplay,
  placeholder,
  selectedHandler,
  handleModifyHandler,
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
        onChange={handleModifyHandler(selectedHandler, fieldName)}
        value={selectedHandler[fieldName]}
        autoComplete="off"
        spellCheck="false"
      />
    </div>
  )
}

const {func, shape, string, bool} = PropTypes

HandlerInput.propTypes = {
  fieldName: string.isRequired,
  fieldDisplay: string,
  placeholder: string,
  redacted: bool,
  editable: bool,
  selectedHandler: shape({}).isRequired,
  handleModifyHandler: func.isRequired,
}

export default HandlerInput
