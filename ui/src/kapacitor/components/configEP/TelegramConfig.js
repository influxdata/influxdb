import React, {PropTypes} from 'react'
import EndpointInput from 'src/kapacitor/components/EndpointInput'
import EndpointCheckbox from 'src/kapacitor/components/EndpointCheckbox'

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
        <EndpointCheckbox
          selectedEndpoint={selectedEndpoint}
          handleModifyEndpoint={handleModifyEndpoint}
          fieldName="disableWebPagePreview"
          fieldDisplay="Disable web page preview"
        />
        <EndpointCheckbox
          selectedEndpoint={selectedEndpoint}
          handleModifyEndpoint={handleModifyEndpoint}
          fieldName="disableNotification"
          fieldDisplay="Disable notification"
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
