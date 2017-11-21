import React, {PropTypes} from 'react'
import EndpointInput from 'src/kapacitor/components/EndpointInput'
import EmptyEndpoint from 'src/kapacitor/components/EmptyEndpoint'

const HipchatConfig = ({
  selectedEndpoint,
  handleModifyEndpoint,
  configLink,
}) => {
  return selectedEndpoint.enabled
    ? <div className="endpoint-tab-contents">
        <div className="endpoint-tab--parameters">
          <h4>Parameters:</h4>
          <EndpointInput
            selectedEndpoint={selectedEndpoint}
            handleModifyEndpoint={handleModifyEndpoint}
            fieldName="url"
            fieldDisplay="Subdomain Url"
            placeholder="Ex: hipchat_subdomain"
            editable={false}
          />
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
            redacted={true}
          />
        </div>
      </div>
    : <EmptyEndpoint configLink={configLink} />
}

const {func, shape, string} = PropTypes

HipchatConfig.propTypes = {
  selectedEndpoint: shape({}).isRequired,
  handleModifyEndpoint: func.isRequired,
  configLink: string,
}

export default HipchatConfig
