import React, {PropTypes} from 'react'
import EndpointInput from 'src/kapacitor/components/EndpointInput'

const VictoropsConfig = ({selectedEndpoint, handleModifyEndpoint}) => {
  return (
    <div className="endpoint-tab-contents">
      <div className="endpoint-tab--parameters">
        <h4>Optional Parameters</h4>
        <EndpointInput
          selectedEndpoint={selectedEndpoint}
          handleModifyEndpoint={handleModifyEndpoint}
          fieldName="routingKey"
          fieldDisplay="Routing Key:"
          placeholder="Ex: routing_key"
        />
      </div>
    </div>
  )
}

const {func, shape} = PropTypes

VictoropsConfig.propTypes = {
  selectedEndpoint: shape({}).isRequired,
  handleModifyEndpoint: func.isRequired,
}

export default VictoropsConfig
