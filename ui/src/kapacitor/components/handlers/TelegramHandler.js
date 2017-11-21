import React, {PropTypes} from 'react'
import HandlerInput from 'src/kapacitor/components/HandlerInput'
import HandlerCheckbox from 'src/kapacitor/components/HandlerCheckbox'
import HandlerEmpty from 'src/kapacitor/components/HandlerEmpty'

const TelegramHandler = ({
  selectedHandler,
  handleModifyHandler,
  configLink,
}) => {
  return selectedHandler.enabled
    ? <div className="endpoint-tab-contents">
        <div className="endpoint-tab--parameters">
          <h4>Optional Parameters</h4>
          <HandlerInput
            selectedHandler={selectedHandler}
            handleModifyHandler={handleModifyHandler}
            fieldName="chatId"
            fieldDisplay="Chat ID:"
            placeholder="Ex: ??????"
          />
          <HandlerInput
            selectedHandler={selectedHandler}
            handleModifyHandler={handleModifyHandler}
            fieldName="parseMode"
            fieldDisplay="Parse Mode:"
            placeholder="Ex: Markdown"
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
    : <HandlerEmpty configLink={configLink} />
}

const {func, shape, string} = PropTypes

TelegramHandler.propTypes = {
  selectedHandler: shape({}).isRequired,
  handleModifyHandler: func.isRequired,
  configLink: string,
}

export default TelegramHandler
