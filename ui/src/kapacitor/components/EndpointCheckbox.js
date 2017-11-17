import React, {PropTypes} from 'react'

const EndpointCheckbox = ({
  fieldName,
  fieldDisplay,
  selectedEndpoint,
  handleModifyEndpoint,
}) => {
  return (
    <div className="form-group">
      <div className="form-control-static">
        <input
          name={fieldName}
          id={fieldName}
          type="checkbox"
          defaultChecked={selectedEndpoint[fieldName]}
          onClick={handleModifyEndpoint(selectedEndpoint, fieldName)}
        />
        <label htmlFor={fieldName}>
          {fieldDisplay}
        </label>
      </div>
    </div>
  )
}

const {func, shape, string, bool} = PropTypes

EndpointCheckbox.propTypes = {
  fieldName: string,
  fieldDisplay: string,
  defaultChecked: bool,
  selectedEndpoint: shape({}).isRequired,
  handleModifyEndpoint: func.isRequired,
}

export default EndpointCheckbox
