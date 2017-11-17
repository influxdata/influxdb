import React, {PropTypes} from 'react'
import EndpointInput from 'src/kapacitor/components/EndpointInput'

const PushoverConfig = ({selectedEndpoint, handleModifyEndpoint}) => {
  return (
    <div className="endpoint-tab-contents">
      <div className="endpoint-tab--parameters">
        <h4>Optional Parameters</h4>
        <EndpointInput
          selectedEndpoint={selectedEndpoint}
          handleModifyEndpoint={handleModifyEndpoint}
          fieldName="userKey"
          fieldDisplay="User Key"
          placeholder="Ex: the_key"
        />
        <EndpointInput
          selectedEndpoint={selectedEndpoint}
          handleModifyEndpoint={handleModifyEndpoint}
          fieldName="device"
          fieldDisplay="Device: (comma separated)"
          placeholder="Ex: dv1, dv2"
        />
        <EndpointInput
          selectedEndpoint={selectedEndpoint}
          handleModifyEndpoint={handleModifyEndpoint}
          fieldName="title"
          fieldDisplay="Title:"
          placeholder="Ex: Important Alert"
        />
      </div>
      <div className="optional-alert-parameters">
        <EndpointInput
          selectedEndpoint={selectedEndpoint}
          handleModifyEndpoint={handleModifyEndpoint}
          fieldName="url"
          fieldDisplay="URL:"
          placeholder="Ex: https://influxdata.com"
        />
        <EndpointInput
          selectedEndpoint={selectedEndpoint}
          handleModifyEndpoint={handleModifyEndpoint}
          fieldName="urlTitle"
          fieldDisplay="URL Title:"
          placeholder="Ex: InfluxData"
        />
        <EndpointInput
          selectedEndpoint={selectedEndpoint}
          handleModifyEndpoint={handleModifyEndpoint}
          fieldName="sound"
          fieldDisplay="Sound:"
          placeholder="Ex: alien"
        />
      </div>
    </div>
  )
}

const {func, shape} = PropTypes

PushoverConfig.propTypes = {
  selectedEndpoint: shape({}).isRequired,
  handleModifyEndpoint: func.isRequired,
}

export default PushoverConfig
