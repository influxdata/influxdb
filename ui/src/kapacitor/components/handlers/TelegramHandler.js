import React, {PropTypes} from 'react'
import HandlerInput from 'src/kapacitor/components/HandlerInput'
import HandlerCheckbox from 'src/kapacitor/components/HandlerCheckbox'
import HandlerEmpty from 'src/kapacitor/components/HandlerEmpty'

const TelegramHandler = ({
  selectedHandler,
  handleModifyHandler,
  onGoToConfig,
  validationError,
}) =>
  selectedHandler.enabled
    ? <div className="endpoint-tab-contents">
        <div className="endpoint-tab--parameters">
          <h4>Parameters from Kapacitor Configuration</h4>
          <div className="faux-form">
            <HandlerInput
              selectedHandler={selectedHandler}
              handleModifyHandler={handleModifyHandler}
              fieldName="token"
              fieldDisplay="Token"
              placeholder=""
              disabled={true}
              redacted={true}
              fieldColumns="col-md-12"
            />
          </div>
        </div>
        <div className="endpoint-tab--parameters">
          <h4>Parameters for this Alert Handler</h4>
          <div className="faux-form">
            <HandlerInput
              selectedHandler={selectedHandler}
              handleModifyHandler={handleModifyHandler}
              fieldName="chatId"
              fieldDisplay="Chat ID:"
              placeholder="ex: chat_id"
            />
            <HandlerInput
              selectedHandler={selectedHandler}
              handleModifyHandler={handleModifyHandler}
              fieldName="parseMode"
              fieldDisplay="Parse Mode:"
              placeholder="ex: Markdown or HTML"
            />
            <HandlerCheckbox
              selectedHandler={selectedHandler}
              handleModifyHandler={handleModifyHandler}
              fieldName="disableWebPagePreview"
              fieldDisplay="Disable web page preview"
            />
            <HandlerCheckbox
              selectedHandler={selectedHandler}
              handleModifyHandler={handleModifyHandler}
              fieldName="disableNotification"
              fieldDisplay="Disable notification"
            />
          </div>
        </div>
      </div>
    : <HandlerEmpty
        onGoToConfig={onGoToConfig}
        validationError={validationError}
      />

const {func, shape, string} = PropTypes

TelegramHandler.propTypes = {
  selectedHandler: shape({}).isRequired,
  handleModifyHandler: func.isRequired,
  onGoToConfig: func.isRequired,
  validationError: string.isRequired,
}

export default TelegramHandler
