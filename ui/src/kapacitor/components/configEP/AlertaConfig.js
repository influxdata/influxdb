import React, {PropTypes} from 'react'
import EndpointInput from 'src/kapacitor/components/EndpointInput'
import EmptyEndpoint from 'src/kapacitor/components/EmptyEndpoint'

const AlertaConfig = ({selectedEndpoint, handleModifyEndpoint, configLink}) => {
  return selectedEndpoint.enabled
    ? <div className="endpoint-tab-contents">
        <div className="endpoint-tab--parameters">
          <h4>Parameters from Kapacitor Configuration</h4>
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
            fieldName="origin"
            fieldDisplay="Origin"
            placeholder="Ex: origin"
          />
          <EndpointInput
            selectedEndpoint={selectedEndpoint}
            handleModifyEndpoint={handleModifyEndpoint}
            fieldName="token"
            fieldDisplay="Token"
            placeholder="Ex: my_token"
            redacted={true}
          />
        </div>
        <div className="endpoint-tab--parameters">
          <h4>Optional Parameters</h4>
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
            fieldName="service"
            fieldDisplay="Service"
            placeholder="Ex: service_name"
          />
        </div>
      </div>
    : <EmptyEndpoint configLink={configLink} />
}

const {func, shape, string} = PropTypes

AlertaConfig.propTypes = {
  selectedEndpoint: shape({}).isRequired,
  handleModifyEndpoint: func.isRequired,
  configLink: string,
}

export default AlertaConfig
