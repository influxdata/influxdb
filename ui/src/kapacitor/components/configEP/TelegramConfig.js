import React, {PropTypes} from 'react'
import EndpointInput from 'src/kapacitor/components/EndpointInput'
import EndpointCheckbox from 'src/kapacitor/components/EndpointCheckbox'
import EmptyEndpoint from 'src/kapacitor/components/EmptyEndpoint'

const TelegramConfig = ({selectedEndpoint, handleModifyEndpoint}) => {
  return selectedEndpoint.enabled
    ? <div className="endpoint-tab-contents">
        <div className="endpoint-tab--parameters">
          <h4>Optional Parameters</h4>
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
    : <EmptyEndpoint />
}

const {func, shape} = PropTypes

TelegramConfig.propTypes = {
  selectedEndpoint: shape({}).isRequired,
  handleModifyEndpoint: func.isRequired,
}

export default TelegramConfig
