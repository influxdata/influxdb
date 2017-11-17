import React, {PropTypes} from 'react'
import EndpointInput from 'src/kapacitor/components/EndpointInput'
import EndpointCheckbox from 'src/kapacitor/components/EndpointCheckbox'

const HttpConfig = ({selectedEndpoint, handleModifyEndpoint}) => {
  return (
    <div className="rule-section--row rule-section--border-bottom">
      <p>Alert Parameters:</p>
      <div className="optional-alert-parameters">
        <EndpointInput
          selectedEndpoint={selectedEndpoint}
          handleModifyEndpoint={handleModifyEndpoint}
          fieldName="url"
          fieldDisplay="POST URL"
          placeholder="Ex: http://example.com/api/alert"
        />
        <EndpointCheckbox
          selectedEndpoint={selectedEndpoint}
          handleModifyEndpoint={handleModifyEndpoint}
          fieldName="captureResponse"
          fieldDisplay="Capture response"
        />
      </div>
    </div>
  )
}

const {func, shape} = PropTypes

HttpConfig.propTypes = {
  selectedEndpoint: shape({}).isRequired,
  handleModifyEndpoint: func.isRequired,
}

export default HttpConfig
