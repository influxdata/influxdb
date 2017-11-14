import React, {PropTypes} from 'react'
import EndpointInput from 'src/kapacitor/components/EndpointInput'

const SensuConfig = ({selectedEndpoint, handleModifyEndpoint}) => {
  return (
    <div className="rule-section--row rule-section--border-bottom">
      <p>Alert Parameters:</p>
      <div className="optional-alert-parameters">
        <EndpointInput
          selectedEndpoint={selectedEndpoint}
          handleModifyEndpoint={handleModifyEndpoint}
          fieldName="source"
          fieldDisplay="Source"
          placeholder="Ex: my_source"
        />
        <EndpointInput
          selectedEndpoint={selectedEndpoint}
          handleModifyEndpoint={handleModifyEndpoint}
          fieldName="handlers"
          fieldDisplay="Handlers"
          placeholder="Ex: my_handlers"
        />
      </div>
    </div>
  )
}

const {func, shape} = PropTypes

SensuConfig.propTypes = {
  selectedEndpoint: shape({}).isRequired,
  handleModifyEndpoint: func.isRequired,
}

export default SensuConfig
