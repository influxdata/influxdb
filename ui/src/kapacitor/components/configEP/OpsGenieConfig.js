import React, {PropTypes} from 'react'
import EndpointInput from 'src/kapacitor/components/EndpointInput'

const OpsgenieConfig = ({selectedEndpoint, handleModifyEndpoint}) => {
  return (
    <div className="rule-section--row rule-section--border-bottom">
      <p>Alert Parameters:</p>
      <div className="optional-alert-parameters">
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
  )
}

const {func, shape} = PropTypes

OpsgenieConfig.propTypes = {
  selectedEndpoint: shape({}).isRequired,
  handleModifyEndpoint: func.isRequired,
}

export default OpsgenieConfig
