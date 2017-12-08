import React, {PropTypes} from 'react'
import HandlerInput from 'src/kapacitor/components/HandlerInput'
import HandlerEmpty from 'src/kapacitor/components/HandlerEmpty'

const SlackHandler = ({
  selectedHandler,
  handleModifyHandler,
  onGoToConfig,
  validationError,
}) =>
  selectedHandler.enabled
    ? <div className="endpoint-tab-contents">
        <div className="endpoint-tab--parameters">
          <h4 className="u-flex u-jc-space-between">
            Parameters from Kapacitor Configuration
            <div className="btn btn-default btn-sm" onClick={onGoToConfig}>
              <span className="icon cog-thick" />
              Edit Kapacitor Configuration
            </div>
          </h4>
          <div className="faux-form">
            <HandlerInput
              selectedHandler={selectedHandler}
              handleModifyHandler={handleModifyHandler}
              fieldName="url"
              fieldDisplay="Webhook URL:"
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
              fieldName="channel"
              fieldDisplay="Channel:"
              placeholder="ex: #my_favorite_channel"
              fieldColumns="col-md-4"
            />
            <HandlerInput
              selectedHandler={selectedHandler}
              handleModifyHandler={handleModifyHandler}
              fieldName="username"
              fieldDisplay="Username:"
              placeholder="ex: my_favorite_username"
              fieldColumns="col-md-4"
            />
            <HandlerInput
              selectedHandler={selectedHandler}
              handleModifyHandler={handleModifyHandler}
              fieldName="iconEmoji"
              fieldDisplay="Emoji:"
              placeholder="ex: :thumbsup:"
              fieldColumns="col-md-4"
            />
          </div>
        </div>
      </div>
    : <HandlerEmpty
        onGoToConfig={onGoToConfig}
        validationError={validationError}
      />

const {func, shape, string} = PropTypes

SlackHandler.propTypes = {
  selectedHandler: shape({}).isRequired,
  handleModifyHandler: func.isRequired,
  onGoToConfig: func.isRequired,
  validationError: string.isRequired,
}

export default SlackHandler
