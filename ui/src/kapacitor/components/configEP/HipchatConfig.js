import React, {PropTypes} from 'react'
import EndpointInput from 'src/kapacitor/components/EndpointInput'
import EmptyEndpoint from 'src/kapacitor/components/EmptyEndpoint'

const HipchatConfig = ({selectedEndpoint, handleModifyEndpoint}) => {
  return selectedEndpoint.enabled
    ? <div className="endpoint-tab-contents">
        <div className="endpoint-tab--parameters">
          <h4>Optional Parameters</h4>
          <EndpointInput
            selectedEndpoint={selectedEndpoint}
            handleModifyEndpoint={handleModifyEndpoint}
            fieldName="room"
            fieldDisplay="Room:"
            placeholder="Ex: room_name"
          />
          <EndpointInput
            selectedEndpoint={selectedEndpoint}
            handleModifyEndpoint={handleModifyEndpoint}
            fieldName="token"
            fieldDisplay="Token:"
            placeholder="Ex: the_token"
          />
        </div>
      </div>
    : <EmptyEndpoint />
}

const {func, shape} = PropTypes

HipchatConfig.propTypes = {
  selectedEndpoint: shape({}).isRequired,
  handleModifyEndpoint: func.isRequired,
}

export default HipchatConfig
