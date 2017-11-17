import React, {PropTypes} from 'react'
import EndpointInput from 'src/kapacitor/components/EndpointInput'

const LogConfig = ({selectedEndpoint, handleModifyEndpoint}) => {
  return (
    <div className="endpoint-tab-contents">
      <div className="endpoint-tab--parameters">
        <h4>Optional Parameters</h4>
        <EndpointInput
          selectedEndpoint={selectedEndpoint}
          handleModifyEndpoint={handleModifyEndpoint}
          fieldName="filePath"
          fieldDisplay="Log File Path:"
          placeholder="Ex: /tmp/alerts.log"
        />
      </div>
    </div>
  )
}

const {func, shape} = PropTypes

LogConfig.propTypes = {
  selectedEndpoint: shape({}).isRequired,
  handleModifyEndpoint: func.isRequired,
}

export default LogConfig
