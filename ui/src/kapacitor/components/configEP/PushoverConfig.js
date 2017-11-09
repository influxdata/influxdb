import React, {PropTypes} from 'react'
import EndpointInput from 'src/kapacitor/components/EndpointInput'

const PushoverConfig = ({selectedEndpoint, handleModifyEndpoint}) => {
  return (
    <div className="rule-section--row rule-section--border-bottom">
      <p>Alert Parameters:</p>
      <div className="optional-alert-parameters">
        <EndpointInput
          selectedEndpoint={selectedEndpoint}
          handleModifyEndpoint={handleModifyEndpoint}
          fieldName="device"
          fieldDisplay="Device: (comma separated)"
          placeholder="Ex: dv1,dv2"
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
          fieldName="URL"
          fieldDisplay="URL:"
          placeholder="Ex: https://influxdata.com"
        />
        <EndpointInput
          selectedEndpoint={selectedEndpoint}
          handleModifyEndpoint={handleModifyEndpoint}
          fieldName="URLTitle"
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
