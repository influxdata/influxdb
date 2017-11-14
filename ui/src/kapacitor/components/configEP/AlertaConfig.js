import React, {PropTypes} from 'react'
import EndpointInput from 'src/kapacitor/components/EndpointInput'

const AlertaConfig = ({selectedEndpoint, handleModifyEndpoint}) => {
  return (
    <div className="rule-section--row rule-section--border-bottom">
      <p>Alert Parameters:</p>
      <div className="optional-alert-parameters">
        <EndpointInput
          selectedEndpoint={selectedEndpoint}
          handleModifyEndpoint={handleModifyEndpoint}
          fieldName="token"
          fieldDisplay="Token"
          placeholder="Ex: my_token"
        />
        <EndpointInput
          selectedEndpoint={selectedEndpoint}
          handleModifyEndpoint={handleModifyEndpoint}
          fieldName="resource"
          fieldDisplay="Resource"
          placeholder="Ex: my_resource"
        />
        <EndpointInput
          selectedEndpoint={selectedEndpoint}
          handleModifyEndpoint={handleModifyEndpoint}
          fieldName="event"
          fieldDisplay="Event"
          placeholder="Ex: event"
        />
        <EndpointInput
          selectedEndpoint={selectedEndpoint}
          handleModifyEndpoint={handleModifyEndpoint}
          fieldName="environment"
          fieldDisplay="Environment"
          placeholder="Ex: environment"
        />
        <EndpointInput
          selectedEndpoint={selectedEndpoint}
          handleModifyEndpoint={handleModifyEndpoint}
          fieldName="group"
          fieldDisplay="Group"
          placeholder="Ex: group_name"
        />
        <EndpointInput
          selectedEndpoint={selectedEndpoint}
          handleModifyEndpoint={handleModifyEndpoint}
          fieldName="value"
          fieldDisplay="Value"
          placeholder="Ex: value"
        />
        <EndpointInput
          selectedEndpoint={selectedEndpoint}
          handleModifyEndpoint={handleModifyEndpoint}
          fieldName="origin"
          fieldDisplay="Origin"
          placeholder="Ex: origin"
        />
        <EndpointInput
          selectedEndpoint={selectedEndpoint}
          handleModifyEndpoint={handleModifyEndpoint}
          fieldName="service"
          fieldDisplay="Service"
          placeholder="Ex: my_token"
        />
        <EndpointInput
          selectedEndpoint={selectedEndpoint}
          handleModifyEndpoint={handleModifyEndpoint}
          fieldName="timeout"
          fieldDisplay="Timeout"
          placeholder="Ex: timeout_duration"
        />
      </div>
    </div>
  )
}

const {func, shape} = PropTypes

AlertaConfig.propTypes = {
  selectedEndpoint: shape({}).isRequired,
  handleModifyEndpoint: func.isRequired,
}

export default AlertaConfig
