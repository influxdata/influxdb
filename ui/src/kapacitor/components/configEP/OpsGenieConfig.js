import React, {PropTypes} from 'react'
import EndpointInput from 'src/kapacitor/components/EndpointInput'
import EmptyEndpoint from 'src/kapacitor/components/EmptyEndpoint'

const OpsgenieConfig = ({selectedEndpoint, handleModifyEndpoint}) => {
  return selectedEndpoint.enabled
    ? <div className="endpoint-tab-contents">
        <div className="endpoint-tab--parameters">
          <h4>Optional Parameters</h4>
          <EndpointInput
            selectedEndpoint={selectedEndpoint}
            handleModifyEndpoint={handleModifyEndpoint}
            fieldName="teams"
            fieldDisplay="Teams"
            placeholder="Ex: teams_name"
          />
          <EndpointInput
            selectedEndpoint={selectedEndpoint}
            handleModifyEndpoint={handleModifyEndpoint}
            fieldName="recipients"
            fieldDisplay="Recipients"
            placeholder="Ex: recipients_name"
          />
        </div>
      </div>
    : <EmptyEndpoint />
}

const {func, shape} = PropTypes

OpsgenieConfig.propTypes = {
  selectedEndpoint: shape({}).isRequired,
  handleModifyEndpoint: func.isRequired,
}

export default OpsgenieConfig
