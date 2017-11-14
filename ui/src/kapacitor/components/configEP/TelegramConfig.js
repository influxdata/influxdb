import React, {PropTypes} from 'react'
import EndpointInput from 'src/kapacitor/components/EndpointInput'

const TelegramConfig = ({selectedEndpoint, handleModifyEndpoint}) => {
  return (
    <div className="rule-section--row rule-section--border-bottom">
      <p>Alert Parameters:</p>
      <div className="optional-alert-parameters">
        <EndpointInput
          selectedEndpoint={selectedEndpoint}
          handleModifyEndpoint={handleModifyEndpoint}
          fieldName="chatId"
          fieldDisplay="Chat ID:"
          placeholder="Ex: ??????"
        />
        <EndpointInput
          selectedEndpoint={selectedEndpoint}
          handleModifyEndpoint={handleModifyEndpoint}
          fieldName="parseMode"
          fieldDisplay="Parse Mode:"
          placeholder="Ex: Markdown"
        />
      </div>
    </div>
  )
}

const {func, shape} = PropTypes

TelegramConfig.propTypes = {
  selectedEndpoint: shape({}).isRequired,
  handleModifyEndpoint: func.isRequired,
}

export default TelegramConfig
