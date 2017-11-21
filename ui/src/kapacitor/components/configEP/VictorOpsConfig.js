import React, {PropTypes} from 'react'
import EndpointInput from 'src/kapacitor/components/EndpointInput'
import EmptyEndpoint from 'src/kapacitor/components/EmptyEndpoint'

const VictoropsConfig = ({
  selectedEndpoint,
  handleModifyEndpoint,
  configLink,
}) => {
  return selectedEndpoint.enabled
    ? <div className="endpoint-tab-contents">
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
    : <EmptyEndpoint configLink={configLink} />
}

const {func, shape, string} = PropTypes

VictoropsConfig.propTypes = {
  selectedEndpoint: shape({}).isRequired,
  handleModifyEndpoint: func.isRequired,
  configLink: string,
}

export default VictoropsConfig
