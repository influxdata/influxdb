import React, {PropTypes} from 'react'
import EndpointInput from 'src/kapacitor/components/EndpointInput'

const ExecConfig = ({selectedEndpoint, handleModifyEndpoint}) => {
  return (
    <div className="endpoint-tab-contents">
      <div className="endpoint-tab--parameters">
        <h4>Optional Parameters</h4>
        <EndpointInput
          selectedEndpoint={selectedEndpoint}
          handleModifyEndpoint={handleModifyEndpoint}
          fieldName="command"
          fieldDisplay="Command (arguments separated by spaces):"
          placeholder="Ex: command argument"
        />
      </div>
    </div>
  )
}

const {func, shape} = PropTypes

ExecConfig.propTypes = {
  selectedEndpoint: shape({}).isRequired,
  handleModifyEndpoint: func.isRequired,
}

export default ExecConfig
