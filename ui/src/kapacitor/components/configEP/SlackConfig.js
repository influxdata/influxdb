import React, {PropTypes} from 'react'
import EndpointInput from 'src/kapacitor/components/EndpointInput'

const SlackConfig = ({selectedEndpoint, handleModifyEndpoint}) => {
  return (
    <div className="endpoint-tab-contents">
      <div className="endpoint-tab--parameters">
        <h4>Optional Parameters</h4>
        <EndpointInput
          selectedEndpoint={selectedEndpoint}
          handleModifyEndpoint={handleModifyEndpoint}
          fieldName="channel"
          fieldDisplay="Channel:"
          placeholder="Ex: #my_favorite_channel"
        />
        <EndpointInput
          selectedEndpoint={selectedEndpoint}
          handleModifyEndpoint={handleModifyEndpoint}
          fieldName="username"
          fieldDisplay="Username:"
          placeholder="Ex: my_favorite_username"
        />
        <EndpointInput
          selectedEndpoint={selectedEndpoint}
          handleModifyEndpoint={handleModifyEndpoint}
          fieldName="iconEmoji"
          fieldDisplay="Emoji:"
          placeholder="Ex: :thumbsup:"
        />
      </div>
    </div>
  )
}

const {func, shape} = PropTypes

SlackConfig.propTypes = {
  selectedEndpoint: shape({}).isRequired,
  handleModifyEndpoint: func.isRequired,
}

export default SlackConfig
